"""
SoundWave — Punto de entrada principal
TP Integrador — Bases de Datos Documentales y Clave-Valor

Orquesta la ejecución de todas las fases del proyecto.
La conexión a MongoDB se establece una sola vez y se comparte.

Uso:
    python main.py              # ejecuta todas las fases
    python main.py --fase 1     # ejecuta solo el seed
    python main.py --fase 2     # ejecuta solo los pipelines
    python main.py --fase 3     # ejecuta solo transacciones y CAP
    python main.py --fase 4     # ejecuta solo Redis
"""

import argparse
import sys

# ── Configuración centralizada ──────────────────────────────
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME   = "soundwave"
REDIS_HOST = "localhost"
REDIS_PORT = 6379


def banner(titulo):
    print("\n" + "═" * 60)
    print(f"  {titulo}")
    print("═" * 60)


def ejecutar_fase1():
    banner("FASE 1 — Seed: Modelado y carga de datos")
    import seed
    db = seed.run(uri=MONGO_URI, db_name=DB_NAME)
    print("✓ Fase 1 completada\n")
    return db


def ejecutar_fase2(db):
    banner("FASE 2 — Aggregation Framework + Índices ESR")
    import fase2
    fase2.crear_indices(db)
    fase2.main(db)
    print("✓ Fase 2 completada\n")


def ejecutar_fase3(db):
    banner("FASE 3 — Transacciones ACID + CAP + Sharding")
    import fase3
    fase3.main(db)
    print("✓ Fase 3 completada\n")


def ejecutar_fase4():
    banner("FASE 4 — Redis: Caché y estructuras de datos")
    import fase4
    fase4.main(host=REDIS_HOST, port=REDIS_PORT)
    print("✓ Fase 4 completada\n")


# ── Main ────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="SoundWave — TP Integrador NoSQL"
    )
    parser.add_argument(
        "--fase",
        type=int,
        choices=[1, 2, 3, 4],
        help="Ejecutar una fase específica (1-4). Sin argumento ejecuta todas.",
    )
    args = parser.parse_args()

    print("\n╔══════════════════════════════════════════════════════╗")
    print("║         SoundWave — TP Integrador NoSQL              ║")
    print("║   MongoDB + Redis · Plataforma de Música Streaming   ║")
    print("╚══════════════════════════════════════════════════════╝")

    try:
        if args.fase == 1:
            ejecutar_fase1()

        elif args.fase == 2:
            # Fase 2 necesita la BD ya seedeada
            from pymongo import MongoClient
            db = MongoClient(MONGO_URI)[DB_NAME]
            ejecutar_fase2(db)

        elif args.fase == 3:
            from pymongo import MongoClient
            db = MongoClient(MONGO_URI)[DB_NAME]
            ejecutar_fase3(db)

        elif args.fase == 4:
            ejecutar_fase4()

        else:
            # Sin argumento: ejecutar todo en orden
            db = ejecutar_fase1()
            ejecutar_fase2(db)
            ejecutar_fase3(db)
            ejecutar_fase4()

        print("\n" + "═" * 60)
        print("  ✓ Ejecución finalizada correctamente")
        print("═" * 60 + "\n")

    except Exception as e:
        print(f"\n✗ Error durante la ejecución: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
