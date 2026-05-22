"""
SoundWave — Script de Inicialización (Seed)
Fase 1: TP Integrador — Bases de Datos Documentales y Clave-Valor

Uso:
    pip install pymongo
    python seed.py

Requisito: MongoDB corriendo en localhost:27017
"""

from pymongo import MongoClient, ASCENDING
from bson import ObjectId
from datetime import datetime, timedelta
import random

# ─────────────────────────────────────────
# CONEXIÓN
# ─────────────────────────────────────────
client = MongoClient("mongodb://localhost:27017/")
db = client["soundwave"]

# Limpiar colecciones existentes para un seed idempotente
for col in ["users", "artists", "albums", "songs", "playlists"]:
    db[col].drop()

print("Colecciones limpiadas. Insertando datos...")

# ─────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────
def ts(days_ago=0):
    """Retorna un datetime de hace N días."""
    return datetime.utcnow() - timedelta(days=days_ago)


# ─────────────────────────────────────────
# 1. ARTISTAS
# ─────────────────────────────────────────
# Decision de modelado 1: social_links se EMBEBE (1:1, siempre se lee junto al artista)

artist_ids = [ObjectId() for _ in range(3)]

artists = [
    {
        "_id": artist_ids[0],
        "name": "Aurora Waves",
        "genres": ["indie pop", "dream pop"],
        "bio": "Duo argentino de pop etéreo formado en 2018.",
        "monthly_listeners": 1_450_000,
        "social_links": {          # ← EMBEDDING: sub-documento 1:1
            "instagram": "https://instagram.com/aurora_waves",
            "spotify":   "https://open.spotify.com/artist/aurorawaves",
            "twitter":   "https://twitter.com/aurora_waves"
        },
        "top_track_ids": [],       # ← REFERENCING: se completa después
        "created_at": ts(365),
    },
    {
        "_id": artist_ids[1],
        "name": "El Pampero",
        "genres": ["rock nacional", "folk"],
        "bio": "Banda de rock con raíces folclóricas del noroeste argentino.",
        "monthly_listeners": 890_000,
        "social_links": {
            "instagram": "https://instagram.com/elpampero",
            "youtube":   "https://youtube.com/@elpampero",
        },
        "top_track_ids": [],
        "created_at": ts(500),
    },
    {
        "_id": artist_ids[2],
        "name": "Lucía Solar",
        "genres": ["pop latino", "reggaeton"],
        "bio": "Cantautora porteña con influencias urbanas y tropicales.",
        "monthly_listeners": 3_200_000,
        "social_links": {
            "instagram": "https://instagram.com/lucia_solar",
            "tiktok":    "https://tiktok.com/@lucia_solar",
            "spotify":   "https://open.spotify.com/artist/luciasolar"
        },
        "top_track_ids": [],
        "created_at": ts(200),
    },
]

db.artists.insert_many(artists)
print(f"  ✓ {len(artists)} artistas insertados")


# ─────────────────────────────────────────
# 2. ÁLBUMES
# ─────────────────────────────────────────
# Decision de modelado: cover_art se EMBEBE (metadatos de imagen, siempre necesarios al mostrar un álbum)
# track_ids se REFERENCIA (las canciones tienen vida independiente)

album_data = [
    (artist_ids[0], "Mareas de Luz",    "2022-03-15", "album"),
    (artist_ids[0], "EP Ventanas",      "2023-08-01", "ep"),
    (artist_ids[1], "Viento Norte",     "2020-11-20", "album"),
    (artist_ids[1], "Raíces",           "2019-04-10", "album"),
    (artist_ids[2], "Calor de Verano",  "2023-12-01", "album"),
    (artist_ids[2], "Singles 2024",     "2024-05-15", "compilation"),
]

album_ids = [ObjectId() for _ in album_data]

albums = []
for i, (art_id, title, release, alb_type) in enumerate(album_data):
    albums.append({
        "_id": album_ids[i],
        "title": title,
        "artist_id": art_id,               # ← REFERENCING
        "release_date": datetime.fromisoformat(release),
        "type": alb_type,
        "cover_art": {                     # ← EMBEDDING: metadatos de imagen 1:1
            "url": f"https://cdn.soundwave.io/covers/{album_ids[i]}.jpg",
            "width": 640,
            "height": 640,
            "color_palette": ["#1a1a2e", "#e94560"]
        },
        "track_ids": [],                   # ← se completa al insertar songs
        "total_tracks": 0,
        "created_at": ts(random.randint(30, 600)),
    })

db.albums.insert_many(albums)
print(f"  ✓ {len(albums)} álbumes insertados")


# ─────────────────────────────────────────
# 3. CANCIONES
# ─────────────────────────────────────────
# Decision de modelado 1: audio_features se EMBEBE (1:1, siempre se lee junto a la canción)
# Decision de modelado 3: el vínculo con album/artist es por REFERENCIA (ObjectId)

song_titles = [
    ["Orilla", "Espejo Roto", "Cielo de Noche", "Marea Alta", "Polvo de Sal"],
    ["Ventana al Sur", "Lluvia Quieta", "Espacio", "Lo Que Fue"],
    ["Pampa Negra", "Tormenta Seca", "Huella", "Raíz Amarga", "Quilombo"],
    ["Volver al Río", "La Tierra Habla", "Fogón", "Silencio de Puna", "Cruz del Sur"],
    ["Calor", "Noche de Salsa", "Sin Red", "Latidos", "Marea Caliente"],
    ["Contigo o Sin Ti", "Florecer", "La Foto", "Sola y Libre"],
]

song_ids_per_album = []
all_song_ids = []

for alb_idx, album in enumerate(albums):
    titles = song_titles[alb_idx]
    s_ids = []
    songs_batch = []
    for t_idx, title in enumerate(titles):
        sid = ObjectId()
        s_ids.append(sid)
        all_song_ids.append(sid)
        songs_batch.append({
            "_id": sid,
            "title": title,
            "artist_id": album["artist_id"],   # ← REFERENCING
            "album_id": album["_id"],           # ← REFERENCING
            "track_number": t_idx + 1,
            "duration_ms": random.randint(150_000, 310_000),
            "explicit": random.choice([True, False]),
            "play_count": random.randint(1_000, 5_000_000),
            "likes": random.randint(100, 500_000),
            "audio_features": {                # ← EMBEDDING: sub-documento 1:1
                "tempo":        round(random.uniform(70, 160), 1),
                "energy":       round(random.uniform(0.1, 1.0), 3),
                "danceability": round(random.uniform(0.1, 1.0), 3),
                "key":          random.randint(0, 11),
                "valence":      round(random.uniform(0.0, 1.0), 3),
            },
            "created_at": ts(random.randint(30, 500)),
        })
    db.songs.insert_many(songs_batch)
    song_ids_per_album.append(s_ids)
    # Actualizar el álbum con sus track_ids
    db.albums.update_one(
        {"_id": album["_id"]},
        {"$set": {"track_ids": s_ids, "total_tracks": len(s_ids)}}
    )

total_songs = sum(len(x) for x in song_ids_per_album)
print(f"  ✓ {total_songs} canciones insertadas")

# Actualizar top_track_ids de cada artista (las 3 canciones con más plays)
for art_id in artist_ids:
    top = list(
        db.songs.find({"artist_id": art_id}, {"_id": 1}).sort("play_count", -1).limit(3)
    )
    db.artists.update_one(
        {"_id": art_id},
        {"$set": {"top_track_ids": [s["_id"] for s in top]}}
    )


# ─────────────────────────────────────────
# 4. USUARIOS
# ─────────────────────────────────────────
# Decision de modelado 2: play_history se EMBEBE con límite de 50 entradas (Subset Pattern)
# following_ids se REFERENCIA (los artistas existen de forma independiente)

user_names = [
    "martin.garcia", "sofia.lopez", "lucas.rodriguez",
    "valentina.perez", "mateo.fernandez", "camila.gonzalez",
    "nicolas.diaz", "agustina.martinez", "tomás.sanchez", "julieta.romero"
]

user_ids = [ObjectId() for _ in user_names]

users = []
for i, uname in enumerate(user_names):
    # Historial de reproducción embebido (últimas 5-10 escuchas)
    history_count = random.randint(5, 10)
    play_history = [
        {
            "song_id": random.choice(all_song_ids),   # ← REFERENCING dentro de EMBEDDING
            "played_at": ts(random.randint(0, 30)),
            "duration_played_ms": random.randint(30_000, 290_000),
        }
        for _ in range(history_count)
    ]
    # Ordenar por fecha descendente (más reciente primero)
    play_history.sort(key=lambda x: x["played_at"], reverse=True)

    users.append({
        "_id": user_ids[i],
        "email": f"{uname}@mail.com",
        "username": uname,
        "display_name": uname.replace(".", " ").title(),
        "plan": random.choice(["free", "free", "premium"]),
        "following_ids": random.sample(artist_ids, k=random.randint(1, 3)),  # ← REFERENCING
        "play_history": play_history,                                          # ← EMBEDDING con límite
        "created_at": ts(random.randint(30, 700)),
    })

db.users.insert_many(users)
print(f"  ✓ {len(users)} usuarios insertados")


# ─────────────────────────────────────────
# 5. PLAYLISTS
# ─────────────────────────────────────────
# Decision de modelado 3: tracks almacena ObjectIds (REFERENCING)
# Las canciones tienen existencia propia y aparecen en múltiples playlists

playlists_data = [
    ("Lo Mejor del Indie",      user_ids[0], True,  song_ids_per_album[0] + song_ids_per_album[1]),
    ("Rock Argentino Clásico",  user_ids[1], True,  song_ids_per_album[2] + song_ids_per_album[3]),
    ("Mix Personal de Camila",  user_ids[5], False, random.sample(all_song_ids, 8)),
    ("Playlist del Gym",        user_ids[3], False, random.sample(all_song_ids, 6)),
    ("Noche de Pop Latino",     user_ids[2], True,  song_ids_per_album[4] + song_ids_per_album[5]),
]

playlists = []
for name, owner, is_public, tracks in playlists_data:
    playlists.append({
        "_id": ObjectId(),
        "name": name,
        "owner_id": owner,           # ← REFERENCING
        "is_public": is_public,
        "followers": random.randint(0, 15_000),
        "tracks": tracks,            # ← REFERENCING: array de ObjectIds de songs
        "created_at": ts(random.randint(10, 300)),
        "updated_at": ts(random.randint(0, 10)),
    })

db.playlists.insert_many(playlists)
print(f"  ✓ {len(playlists)} playlists insertadas")


# ─────────────────────────────────────────
# 6. ÍNDICES (pre-creados para Fase 2)
# ─────────────────────────────────────────
# Siguiendo la regla ESR (Equality → Sort → Range)

# Canciones: búsqueda por artista, ordenado por play_count (para top tracks)
db.songs.create_index([("artist_id", ASCENDING), ("play_count", ASCENDING)])

# Canciones: búsqueda por álbum y número de track (para reproducción de álbum completo)
db.songs.create_index([("album_id", ASCENDING), ("track_number", ASCENDING)])

# Usuarios: lookup por email (login)
db.users.create_index([("email", ASCENDING)], unique=True)

# Playlists: públicas ordenadas por seguidores
db.playlists.create_index([("is_public", ASCENDING), ("followers", ASCENDING)])

print("  ✓ Índices creados")


# ─────────────────────────────────────────
# RESUMEN
# ─────────────────────────────────────────
print("\n─── Seed completado ───────────────────────────────")
print(f"  artists:   {db.artists.count_documents({})}")
print(f"  albums:    {db.albums.count_documents({})}")
print(f"  songs:     {db.songs.count_documents({})}")
print(f"  users:     {db.users.count_documents({})}")
print(f"  playlists: {db.playlists.count_documents({})}")
print("────────────────────────────────────────────────────")
print("Base de datos 'soundwave' lista para la Fase 2.")
