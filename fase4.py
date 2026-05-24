"""
SoundWave — Fase 4: Aceleración con Redis
TP Integrador — Bases de Datos Documentales y Clave-Valor

Estructuras utilizadas:
  1. Hash        → Sesión activa del usuario (perfil + estado de reproducción)
  2. Sorted Set  → Ranking de canciones más escuchadas en la última hora
  3. List        → Cola de eventos de reproducción pendientes de procesar en MongoDB

Gestión de caché:
  - TTL sesión:  30 minutos, renovado con cada acción (sliding window)
  - TTL ranking: 1 hora por ventana horaria
  - TTL cola:    24 horas como red de seguridad ante caída del worker

Invalidación:
  - Explícita:  DELETE inmediato en logout
  - Coherente:  actualización parcial del Hash al cambiar plan (sin desloguear)

Uso standalone:
    python fase4.py

O desde main.py:
    from fase4 import main
    main(db=db, host="localhost", port=6379)

Requisitos: MongoDB y Redis corriendo en localhost.
"""

import redis
import json
from datetime import datetime
from pymongo import MongoClient

# ─────────────────────────────────────────────────────────────
# TTLs (en segundos)
# ─────────────────────────────────────────────────────────────
TTL_SESION      = 30 * 60       # 30 minutos
TTL_RANKING     = 60 * 60       # 1 hora
TTL_COLA        = 24 * 60 * 60  # 24 horas


# ─────────────────────────────────────────────────────────────
# CONEXIONES
# ─────────────────────────────────────────────────────────────
def get_redis(host="localhost", port=6379):
    r = redis.Redis(host=host, port=port, db=0, decode_responses=True)
    r.ping()
    print("✓ Conexión exitosa a Redis")
    return r


# ─────────────────────────────────────────────────────────────
# ESTRUCTURA 1 — HASH
# Clave: session:{user_id}
#
# Decisión: Hash permite actualizar un campo individual (ej: position_ms
# cada 5 segundos de reproducción) sin reescribir toda la estructura.
# Con un String+JSON habría que deserializar, modificar y reserializar
# en cada tick — costoso bajo alta concurrencia.
#
# TTL sliding window: cada interacción del usuario reinicia los 30 min,
# igual que lo hace Spotify o cualquier plataforma de streaming.
# ─────────────────────────────────────────────────────────────

def crear_sesion(r, user_id: str, username: str, plan: str):
    key = f"session:{user_id}"
    r.hset(key, mapping={
        "username":           username,
        "plan":               plan,
        "current_song_id":    "",
        "current_song_title": "",
        "position_ms":        0,
        "shuffle":            "false",
        "repeat":             "false",
        "login_at":           datetime.utcnow().isoformat(),
    })
    r.expire(key, TTL_SESION)
    return key


def reproducir_cancion(r, user_id: str, song_id: str, song_title: str):
    """Actualiza solo los campos de reproducción y renueva el TTL."""
    key = f"session:{user_id}"
    r.hset(key, mapping={
        "current_song_id":    song_id,
        "current_song_title": song_title,
        "position_ms":        0,
    })
    r.expire(key, TTL_SESION)  # sliding window


def obtener_sesion(r, user_id: str) -> dict:
    key = f"session:{user_id}"
    sesion = r.hgetall(key)
    if sesion:
        r.expire(key, TTL_SESION)
    return sesion


def cerrar_sesion(r, user_id: str):
    """
    Invalidación explícita: DELETE inmediato al hacer logout.
    No se espera al TTL porque un token robado no debe poder usarse
    después de que el usuario cerró sesión.
    """
    r.delete(f"session:{user_id}")


def invalidar_sesion_por_cambio_plan(r, user_id: str, nuevo_plan: str):
    """
    Invalidación coherente con el negocio: al cambiar de plan
    (free → premium) se actualiza solo el campo 'plan' en el Hash.
    El usuario no es deslogueado — solo sus permisos cambian.
    """
    key = f"session:{user_id}"
    if r.exists(key):
        r.hset(key, "plan", nuevo_plan)
        r.expire(key, TTL_SESION)


# ─────────────────────────────────────────────────────────────
# ESTRUCTURA 2 — SORTED SET
# Clave: ranking:top_songs:{YYYY-MM-DDTHH}
#
# Decisión: ZINCRBY actualiza el score en O(log N) y mantiene
# el orden automáticamente. Recalcular el top desde MongoDB en
# cada request requeriría un full scan + sort — inviable a escala.
# La granularidad horaria (clave por hora) es suficiente para
# métricas de popularidad y simplifica la expiración con TTL.
# ─────────────────────────────────────────────────────────────

def _ranking_key() -> str:
    return "ranking:top_songs:" + datetime.utcnow().strftime("%Y-%m-%dT%H")


def registrar_en_ranking(r, song_id: str, song_title: str):
    key = _ranking_key()
    r.zincrby(key, 1, f"{song_id}:{song_title}")
    if r.ttl(key) == -1:
        r.expire(key, TTL_RANKING)


def obtener_top_canciones(r, top_n: int = 10) -> list:
    key = _ranking_key()
    resultados = r.zrevrange(key, 0, top_n - 1, withscores=True)
    top = []
    for member, score in resultados:
        song_id, _, song_title = member.partition(":")
        top.append({
            "song_id":    song_id,
            "song_title": song_title,
            "plays_hora": int(score),
        })
    return top


# ─────────────────────────────────────────────────────────────
# ESTRUCTURA 3 — LIST
# Clave: queue:play_events
#
# Decisión: desacopla la escritura a MongoDB de la reproducción.
# En vez de escribir a Mongo sincrónicamente en cada escucha
# (cuello de botella bajo alta concurrencia), el evento se encola
# en Redis con RPUSH y un worker los procesa en batch con LPOP.
# Esto permite absorber picos de tráfico sin degradar la experiencia.
# TTL de 24h garantiza que si el worker cae, los eventos no se pierden.
# ─────────────────────────────────────────────────────────────

QUEUE_KEY = "queue:play_events"


def encolar_evento(r, user_id: str, song_id: str, song_title: str, duration_ms: int):
    evento = json.dumps({
        "user_id":    user_id,
        "song_id":    song_id,
        "song_title": song_title,
        "duration_ms": duration_ms,
        "played_at":  datetime.utcnow().isoformat(),
    })
    r.rpush(QUEUE_KEY, evento)
    if r.ttl(QUEUE_KEY) == -1:
        r.expire(QUEUE_KEY, TTL_COLA)


def procesar_cola(r, batch_size: int = 5) -> list:
    """
    Simula el worker que consume eventos y los persistiría en MongoDB
    (actualizar play_count, agregar entrada a play_history).
    """
    eventos = []
    for _ in range(batch_size):
        raw = r.lpop(QUEUE_KEY)
        if raw is None:
            break
        eventos.append(json.loads(raw))
    return eventos


# ─────────────────────────────────────────────────────────────
# MAIN — usa datos reales del seed
# ─────────────────────────────────────────────────────────────

def main(db=None, host="localhost", port=6379):
    r = get_redis(host, port)

    # Conectar a MongoDB si no se recibió db desde main.py
    if db is None:
        db = MongoClient("mongodb://localhost:27017/")["soundwave"]

    print("\n" + "═" * 60)
    print("  FASE 4 — Redis: Caché y Estructuras de Datos")
    print("  Hash · Sorted Set · List")
    print("═" * 60)

    # ── Leer datos reales del seed ───────────────────────────
    usuarios = list(db.users.find({}, {"_id": 1, "username": 1, "plan": 1}).limit(3))
    canciones = list(db.songs.find({}, {"_id": 1, "title": 1}).limit(5))

    if not usuarios or not canciones:
        print("✗ No hay datos en MongoDB. Ejecutá primero el seed (--fase 1).")
        return

    # ── [1] Crear sesiones desde usuarios reales ─────────────
    print("\n── [1] HASH — Crear sesiones de usuario ────────────")
    for u in usuarios:
        key = crear_sesion(r, str(u["_id"]), u["username"], u["plan"])
        print(f"  ✓ {key}  [{u['plan']}]  TTL: {r.ttl(key)}s")

    # ── [2] Simular reproducciones ───────────────────────────
    print("\n── [2] Reproducciones → Hash + Sorted Set + List ───")
    import random
    reproducciones = [
        (usuarios[0], canciones[0]),
        (usuarios[0], canciones[1]),
        (usuarios[1], canciones[0]),
        (usuarios[1], canciones[2]),
        (usuarios[2], canciones[0]),
        (usuarios[0], canciones[0]),  # Orilla escuchada 3 veces
        (usuarios[2], canciones[3]),
    ]

    for u, s in reproducciones:
        uid  = str(u["_id"])
        sid  = str(s["_id"])
        dur  = random.randint(120_000, 280_000)

        reproducir_cancion(r, uid, sid, s["title"])       # Hash
        registrar_en_ranking(r, sid, s["title"])           # Sorted Set
        encolar_evento(r, uid, sid, s["title"], dur)       # List

        print(f"  ♪ {u['username']:<25} → '{s['title']}'")

    # ── [3] Consultar sesión activa ──────────────────────────
    print("\n── [3] HASH — Leer sesión activa ───────────────────")
    sesion = obtener_sesion(r, str(usuarios[0]["_id"]))
    print(f"  Usuario:    {sesion.get('username')}")
    print(f"  Plan:       {sesion.get('plan')}")
    print(f"  Escuchando: '{sesion.get('current_song_title')}'")
    print(f"  TTL:        {r.ttl('session:' + str(usuarios[0]['_id']))}s restantes")

    # ── [4] Top canciones de la hora ─────────────────────────
    print("\n── [4] SORTED SET — Top canciones esta hora ────────")
    top = obtener_top_canciones(r, top_n=5)
    for i, c in enumerate(top, 1):
        print(f"  {i}. {c['song_title']:<25} — {c['plays_hora']} play(s)")
    print(f"  Clave: {_ranking_key()}  TTL: {r.ttl(_ranking_key())}s")

    # ── [5] Worker consume la cola ───────────────────────────
    print("\n── [5] LIST — Cola de eventos de reproducción ──────")
    print(f"  Eventos pendientes: {r.llen(QUEUE_KEY)}")
    procesados = procesar_cola(r, batch_size=3)
    print(f"  Worker procesó {len(procesados)} evento(s) → (se persistirían en MongoDB)")
    for e in procesados:
        nombre = next((u["username"] for u in usuarios if str(u["_id"]) == e["user_id"]), e["user_id"])
        print(f"    · {nombre:<25} | '{e['song_title']}' | {e['duration_ms']//1000}s")
    print(f"  Eventos restantes en cola: {r.llen(QUEUE_KEY)}")

    # ── [6] Cambio de plan → invalidación coherente ──────────
    print("\n── [6] Invalidación coherente — Cambio de plan ─────")
    uid_test = str(usuarios[0]["_id"])
    plan_anterior = obtener_sesion(r, uid_test).get("plan")
    nuevo_plan = "premium" if plan_anterior == "free" else "free"
    invalidar_sesion_por_cambio_plan(r, uid_test, nuevo_plan)
    plan_nuevo = obtener_sesion(r, uid_test).get("plan")
    print(f"  {usuarios[0]['username']}: '{plan_anterior}' → '{plan_nuevo}'  (sesión activa, no deslogueado)")

    # ── [7] Logout → invalidación explícita ──────────────────
    print("\n── [7] Invalidación explícita — Logout ─────────────")
    uid_logout = str(usuarios[1]["_id"])
    cerrar_sesion(r, uid_logout)
    sesion_post = obtener_sesion(r, uid_logout)
    print(f"  {usuarios[1]['username']}: sesión {'activa' if sesion_post else 'eliminada ✓ (DELETE inmediato)'}")

    print("\n" + "═" * 60)
    print("  Fase 4 completada.")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
