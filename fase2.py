"""
SoundWave — Fase 2: Integración y Procesamiento Avanzado
TP Integrador — Bases de Datos Documentales y Clave-Valor

Contenido:
  1. Conexión con PyMongo (driver oficial)
  2. Índices con estrategia ESR (Equality → Sort → Range)
  3. Pipeline 1: Top 10 canciones más escuchadas del último mes
  4. Pipeline 2: Métricas por género (artistas + plays + seguidores promedio)
  5. Pipeline 3: Análisis de hábitos de escucha por usuario (duración y diversidad)

Uso:
    pip install pymongo
    python fase2.py

Requisito: MongoDB corriendo en localhost:27017 con la BD 'soundwave' ya seedeada.
"""

from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta
import pprint

# ─────────────────────────────────────────────────────────────
# 1. CONEXIÓN — Driver oficial PyMongo
# ─────────────────────────────────────────────────────────────

def get_db():
    """
    Retorna la base de datos 'soundwave'.
    La conexión usa el driver oficial pymongo.MongoClient.
    Se configura serverSelectionTimeoutMS para fallar rápido si
    MongoDB no está disponible.
    """
    client = MongoClient(
        "mongodb://localhost:27017/",
        serverSelectionTimeoutMS=3000
    )
    # Verificar conexión antes de continuar
    client.admin.command("ping")
    print("✓ Conexión exitosa a MongoDB")
    return client["soundwave"]


# ─────────────────────────────────────────────────────────────
# 2. ÍNDICES — Estrategia ESR (Equality → Sort → Range)
# ─────────────────────────────────────────────────────────────

def crear_indices(db):
    """
    Crea índices compuestos siguiendo la regla ESR para las consultas
    críticas de la plataforma.

    Regla ESR:
      E (Equality)  → campos filtrados con $eq o $match exacto
      S (Sort)      → campos usados en $sort
      R (Range)     → campos con $gt, $lt, $gte, $lte, $in

    Los índices del seed.py ya cubren casos simples; aquí se agregan
    los índices compuestos necesarios para los pipelines de Fase 2.
    """

    # ── Índice 1 ──────────────────────────────────────────────
    # Consulta crítica: canciones de un artista (E) con muchos plays (S)
    # creadas en un rango de fechas (R).
    # Pipeline 1 filtra por created_at >= hace 30 días y ordena por play_count.
    # ESR: artist_id (E) → play_count (S) → created_at (R)
    db.songs.create_index(
        [
            ("artist_id",   ASCENDING),   # E — igualdad en el $match
            ("play_count",  DESCENDING),  # S — orden descendente en $sort
            ("created_at",  ASCENDING),   # R — rango en $match ($gte)
        ],
        name="songs_artist_plays_date_esr",
        background=True,
    )

    # ── Índice 2 ──────────────────────────────────────────────
    # Consulta crítica: playlists públicas (E) ordenadas por seguidores (S).
    # Ya existe un índice simple; este cubre también updated_at (R) para
    # filtrar playlists actualizadas recientemente.
    # ESR: is_public (E) → followers (S) → updated_at (R)
    db.playlists.create_index(
        [
            ("is_public",   ASCENDING),   # E
            ("followers",   DESCENDING),  # S
            ("updated_at",  DESCENDING),  # R — rango de fechas recientes
        ],
        name="playlists_public_followers_date_esr",
        background=True,
    )

    # ── Índice 3 ──────────────────────────────────────────────
    # Consulta crítica: historial de un usuario (E) filtrado por fecha (R),
    # para el Pipeline 3 de análisis de hábitos.
    # Como play_history está embebido en users, el índice útil es sobre
    # users._id (E) + play_history.played_at (R) para consultas con $elemMatch.
    # ESR: _id (E) → play_history.played_at (R)
    db.users.create_index(
        [
            ("_id",                      ASCENDING),   # E
            ("play_history.played_at",   DESCENDING),  # R — rango de fechas
        ],
        name="users_id_history_date_esr",
        background=True,
    )

    # ── Índice 4 ──────────────────────────────────────────────
    # Consulta crítica: canciones filtradas por género via artista.
    # Pipeline 2 hace $lookup artists → $unwind genres → $group.
    # Índice en artists.genres (E) + monthly_listeners (S).
    # ESR: genres (E, $in) → monthly_listeners (S)
    db.artists.create_index(
        [
            ("genres",             ASCENDING),   # E — $in sobre array
            ("monthly_listeners",  DESCENDING),  # S
        ],
        name="artists_genres_listeners_esr",
        background=True,
    )

    print("✓ Índices ESR creados/verificados")


# ─────────────────────────────────────────────────────────────
# 3. PIPELINE 1
#    Top 10 canciones más escuchadas en los últimos 30 días
#    Incluye: título, artista, álbum, plays y likes
# ─────────────────────────────────────────────────────────────

def pipeline_top_canciones_mes(db):
    """
    Reporte: Top 10 canciones con mayor play_count entre las canciones
    creadas/actualizadas en el último mes.

    Etapas:
      $match   → Filtra canciones creadas en los últimos 30 días (rango)
      $sort    → Ordena por play_count descendente (usa índice ESR)
      $limit   → Toma las 10 primeras
      $lookup  → Trae el documento del artista (join con artists)
      $lookup  → Trae el documento del álbum   (join con albums)
      $project → Da forma al documento de salida
    """
    hace_30_dias = datetime.utcnow() - timedelta(days=30)

    pipeline = [
        # E+R: created_at en rango — aprovecha índice songs_artist_plays_date_esr
        {
            "$match": {
                "created_at": {"$gte": hace_30_dias}
            }
        },
        # S: ordena por play_count descendente
        {
            "$sort": {"play_count": DESCENDING}
        },
        {
            "$limit": 10
        },
        # Join con artistas
        {
            "$lookup": {
                "from":         "artists",
                "localField":   "artist_id",
                "foreignField": "_id",
                "as":           "artista",
                "pipeline": [
                    {"$project": {"name": 1, "genres": 1}}
                ]
            }
        },
        # Desanidar array artista (siempre tiene 1 elemento)
        {
            "$unwind": {
                "path":                       "$artista",
                "preserveNullAndEmptyArrays": True
            }
        },
        # Join con álbumes
        {
            "$lookup": {
                "from":         "albums",
                "localField":   "album_id",
                "foreignField": "_id",
                "as":           "album",
                "pipeline": [
                    {"$project": {"title": 1, "type": 1}}
                ]
            }
        },
        {
            "$unwind": {
                "path":                       "$album",
                "preserveNullAndEmptyArrays": True
            }
        },
        # Proyección final
        {
            "$project": {
                "_id":          0,
                "cancion":      "$title",
                "artista":      "$artista.name",
                "generos":      "$artista.genres",
                "album":        "$album.title",
                "tipo_album":   "$album.type",
                "play_count":   1,
                "likes":        1,
                "duracion_seg": {"$divide": ["$duration_ms", 1000]},
            }
        },
    ]

    resultados = list(db.songs.aggregate(pipeline))
    return resultados


# ─────────────────────────────────────────────────────────────
# 4. PIPELINE 2
#    Métricas por género musical
#    Para cada género: total de artistas, oyentes mensuales promedio,
#    total de canciones, plays totales y likes promedio por canción.
# ─────────────────────────────────────────────────────────────

def pipeline_metricas_por_genero(db):
    """
    Reporte analítico: métricas agregadas por género musical.

    Etapas:
      $unwind  → Desanida el array genres de cada artista
      $lookup  → Trae las canciones de ese artista (songs join)
      $unwind  → Desanida las canciones
      $group   → Agrupa por género calculando métricas
      $sort    → Ordena por total de plays descendente
      $project → Redondea y da forma a la salida
    """
    pipeline = [
        # Desanidar genres (un artista puede tener varios géneros)
        {
            "$unwind": "$genres"
        },
        # Join: traer todas las canciones del artista
        {
            "$lookup": {
                "from":         "songs",
                "localField":   "_id",
                "foreignField": "artist_id",
                "as":           "canciones"
            }
        },
        # Desanidar canciones para poder agregar a nivel canción
        {
            "$unwind": {
                "path":                       "$canciones",
                "preserveNullAndEmptyArrays": False   # excluir artistas sin canciones
            }
        },
        # Agrupar por género
        {
            "$group": {
                "_id":                  "$genres",
                "total_artistas":       {"$addToSet": "$_id"},          # set de IDs únicos
                "oyentes_mensuales":    {"$avg": "$monthly_listeners"},
                "total_canciones":      {"$sum": 1},
                "plays_totales":        {"$sum": "$canciones.play_count"},
                "likes_promedio":       {"$avg": "$canciones.likes"},
                "plays_promedio":       {"$avg": "$canciones.play_count"},
            }
        },
        # Reemplazar set de artistas por su conteo
        {
            "$addFields": {
                "total_artistas": {"$size": "$total_artistas"}
            }
        },
        {
            "$sort": {"plays_totales": DESCENDING}
        },
        {
            "$project": {
                "_id":               0,
                "genero":            "$_id",
                "total_artistas":    1,
                "oyentes_mensuales_promedio": {"$round": ["$oyentes_mensuales", 0]},
                "total_canciones":   1,
                "plays_totales":     1,
                "plays_promedio":    {"$round": ["$plays_promedio", 0]},
                "likes_promedio":    {"$round": ["$likes_promedio", 0]},
            }
        },
    ]

    resultados = list(db.artists.aggregate(pipeline))
    return resultados


# ─────────────────────────────────────────────────────────────
# 5. PIPELINE 3
#    Análisis de hábitos de escucha por usuario
#    Para cada usuario: canciones únicas escuchadas, tiempo total
#    reproducido, porcentaje de escucha completada y artistas distintos.
# ─────────────────────────────────────────────────────────────

def pipeline_habitos_por_usuario(db):
    """
    Reporte de comportamiento: análisis del historial de reproducción
    embebido en cada usuario.

    Trabaja sobre datos anidados (play_history embebido en users),
    haciendo un $unwind del array interno y un $lookup a songs para
    calcular el porcentaje real de escucha.

    Etapas:
      $match       → Solo usuarios con historial no vacío
      $unwind      → Desanida play_history
      $lookup      → Trae la canción para obtener duration_ms total
      $unwind      → Desanida el array de lookup (1 elemento)
      $group       → Agrupa por usuario calculando métricas de hábito
      $lookup      → Trae el perfil del usuario (username, plan)
      $unwind      → Desanida el perfil
      $sort        → Ordena por tiempo total escuchado
      $project     → Salida final
    """
    pipeline = [
        # Solo usuarios con historial
        {
            "$match": {
                "play_history": {"$exists": True, "$not": {"$size": 0}}
            }
        },
        # Desanidar array embebido play_history
        {
            "$unwind": "$play_history"
        },
        # Join: traer la canción para conocer su duración total
        {
            "$lookup": {
                "from":         "songs",
                "localField":   "play_history.song_id",
                "foreignField": "_id",
                "as":           "cancion_info",
                "pipeline": [
                    {"$project": {"duration_ms": 1, "artist_id": 1, "title": 1}}
                ]
            }
        },
        {
            "$unwind": {
                "path":                       "$cancion_info",
                "preserveNullAndEmptyArrays": True
            }
        },
        # Calcular porcentaje de escucha de esta reproducción
        {
            "$addFields": {
                "pct_escucha": {
                    "$cond": {
                        "if":   {"$gt": ["$cancion_info.duration_ms", 0]},
                        "then": {
                            "$multiply": [
                                {"$divide": [
                                    "$play_history.duration_played_ms",
                                    "$cancion_info.duration_ms"
                                ]},
                                100
                            ]
                        },
                        "else": 0
                    }
                }
            }
        },
        # Agrupar por usuario
        {
            "$group": {
                "_id":                   "$_id",
                "canciones_escuchadas":  {"$addToSet": "$play_history.song_id"},
                "artistas_distintos":    {"$addToSet": "$cancion_info.artist_id"},
                "tiempo_total_ms":       {"$sum": "$play_history.duration_played_ms"},
                "pct_escucha_promedio":  {"$avg": "$pct_escucha"},
                "plan":                  {"$first": "$plan"},
                "username":              {"$first": "$username"},
            }
        },
        # Convertir sets en conteos
        {
            "$addFields": {
                "canciones_unicas":   {"$size": "$canciones_escuchadas"},
                "artistas_distintos": {"$size": "$artistas_distintos"},
                "tiempo_total_min":   {"$divide": ["$tiempo_total_ms", 60000]},
            }
        },
        {
            "$sort": {"tiempo_total_ms": DESCENDING}
        },
        {
            "$project": {
                "_id":                  0,
                "username":             1,
                "plan":                 1,
                "canciones_unicas":     1,
                "artistas_distintos":   1,
                "tiempo_total_min":     {"$round": ["$tiempo_total_min", 1]},
                "pct_escucha_promedio": {"$round": ["$pct_escucha_promedio", 1]},
            }
        },
    ]

    resultados = list(db.users.aggregate(pipeline))
    return resultados


# ─────────────────────────────────────────────────────────────
# MAIN — Ejecuta todo y muestra resultados formateados
# ─────────────────────────────────────────────────────────────

def main(db=None):
    # Si se llama desde main.py, reutiliza la conexión existente
    if db is None:
        db = get_db()

    print("\n" + "═" * 60)
    print("  FASE 2 — SoundWave: Aggregation Framework + Índices ESR")
    print("═" * 60)

    # ── Índices ──
    print("\n[1/4] Creando índices ESR...")
    crear_indices(db)

    # ── Pipeline 1 ──
    print("\n[2/4] Pipeline 1 — Top 10 canciones (últimos 30 días)")
    print("─" * 60)
    top_canciones = pipeline_top_canciones_mes(db)
    if top_canciones:
        for i, c in enumerate(top_canciones, 1):
            print(f"  {i:>2}. {c['cancion']:<25} | {c.get('artista','?'):<20} "
                  f"| plays: {c['play_count']:>9,} | likes: {c['likes']:>7,}")
    else:
        print("  (Sin resultados — ajustá el rango de fechas en el seed)")

    # ── Pipeline 2 ──
    print("\n[3/4] Pipeline 2 — Métricas por género")
    print("─" * 60)
    metricas = pipeline_metricas_por_genero(db)
    for m in metricas:
        print(f"  Género: {m['genero']:<20} | Artistas: {m['total_artistas']} "
              f"| Canciones: {m['total_canciones']:>3} "
              f"| Plays totales: {m['plays_totales']:>10,} "
              f"| Oyentes/mes prom: {m['oyentes_mensuales_promedio']:>10,.0f}")

    # ── Pipeline 3 ──
    print("\n[4/4] Pipeline 3 — Hábitos de escucha por usuario")
    print("─" * 60)
    habitos = pipeline_habitos_por_usuario(db)
    for h in habitos:
        print(f"  {h['username']:<25} [{h['plan']:<7}] "
              f"| Canciones únicas: {h['canciones_unicas']:>2} "
              f"| Artistas: {h['artistas_distintos']:>2} "
              f"| Tiempo: {h['tiempo_total_min']:>6.1f} min "
              f"| % escucha prom: {h['pct_escucha_promedio']:>5.1f}%")

    print("\n" + "═" * 60)
    print("  Fase 2 completada exitosamente.")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
