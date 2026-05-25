"""
SoundWave — Fase 3: Transacciones ACID Multi-documento
TP Integrador — Bases de Datos Documentales y Clave-Valor

Proceso de negocio: suscripción al plan premium.

Involucra tres colecciones de forma atómica:
  1. users -> cambia plan de free a premium
  2. subscriptions -> registra la suscripción activa
  3. payments -> registra el pago asociado

Si cualquier paso falla -> rollback automático.

Uso standalone:
    python fase3.py

O desde main.py:
    from fase3 import main
    main(db=db)
"""

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, PyMongoError
from bson import ObjectId
from datetime import datetime, timezone
import traceback


def get_db(uri="mongodb://localhost:27017/", db_name="soundwave"):
    """Retorna la base de datos soundwave con una conexión básica a MongoDB."""
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("✓ Conexión exitosa a MongoDB")
    return client[db_name]


def upgrade_to_premium(db, user_id: ObjectId, amount: float, payment_method: str) -> dict:
    """
    Suscribe a un usuario al plan premium de forma atómica.
    Toca tres colecciones: users, subscriptions, payments.
    Si cualquier operación falla, se hace rollback de todo.
    """
    user = db.users.find_one({"_id": user_id}, {"plan": 1, "username": 1})
    if not user:
        raise ValueError(f"Usuario {user_id} no encontrado.")
    if user.get("plan") == "premium":
        raise ValueError(f"El usuario '{user.get('username')}' ya tiene plan premium.")

    now = datetime.now(timezone.utc)
    subscription_id = ObjectId()
    payment_id = ObjectId()

    with db.client.start_session() as session:
        try:
            with session.start_transaction():
                result_user = db.users.update_one(
                    {"_id": user_id, "plan": "free"},
                    {
                        "$set": {
                            "plan": "premium",
                            "premium_since": now,
                            "updated_at": now,
                        }
                    },
                    session=session,
                )
                if result_user.matched_count == 0:
                    raise OperationFailure(
                        "No se pudo actualizar el plan (posible condición de carrera)."
                    )

                db.subscriptions.insert_one(
                    {
                        "_id": subscription_id,
                        "user_id": user_id,
                        "plan": "premium",
                        "status": "active",
                        "started_at": now,
                        "renews_at": datetime(
                            now.year + (now.month // 12),
                            (now.month % 12) + 1,
                            now.day,
                            tzinfo=timezone.utc,
                        ),
                        "payment_id": payment_id,
                        "created_at": now,
                    },
                    session=session,
                )

                db.payments.insert_one(
                    {
                        "_id": payment_id,
                        "user_id": user_id,
                        "subscription_id": subscription_id,
                        "amount": amount,
                        "currency": "USD",
                        "method": payment_method,
                        "status": "completed",
                        "paid_at": now,
                        "created_at": now,
                    },
                    session=session,
                )

                session.commit_transaction()

        except (OperationFailure, PyMongoError) as exc:
            print(f"  [ERROR] Transacción abortada: {exc}")
            raise

    return {
        "user_id": user_id,
        "subscription_id": subscription_id,
        "payment_id": payment_id,
        "upgraded_at": now,
    }


def mostrar_resultado(db, txn_result: dict):
    """Verifica y muestra el estado final de los 3 documentos."""
    u = db.users.find_one({"_id": txn_result["user_id"]})
    s = db.subscriptions.find_one({"_id": txn_result["subscription_id"]})
    p = db.payments.find_one({"_id": txn_result["payment_id"]})

    print(f"\n  {'Campo':<30} {'Valor'}")
    print(f"  {'-'*60}")
    print(f"  {'[users] username':<30} {u['username']}")
    print(f"  {'[users] plan':<30} {u['plan']}")
    print(f"  {'[users] premium_since':<30} {u.get('premium_since', '—')}")
    print(f"  {'-'*60}")
    print(f"  {'[subscriptions] status':<30} {s['status']}")
    print(f"  {'[subscriptions] started_at':<30} {s['started_at']}")
    print(f"  {'[subscriptions] renews_at':<30} {s['renews_at']}")
    print(f"  {'-'*60}")
    print(f"  {'[payments] amount':<30} ${p['amount']}")
    print(f"  {'[payments] method':<30} {p['method']}")
    print(f"  {'[payments] status':<30} {p['status']}")
    print(f"  {'[payments] paid_at':<30} {p['paid_at']}")
    print(f"\n  ✓ ATOMICIDAD VERIFICADA: los 3 documentos fueron modificados/creados.")
    print(f"  ✓ subscriptions: {db.subscriptions.count_documents({})} documento(s)")
    print(f"  ✓ payments:      {db.payments.count_documents({})} documento(s)")


def main(db=None):
    """Ejecuta la transacción ACID sobre la BD soundwave."""
    if db is None:
        try:
            db = get_db()
        except ConnectionFailure as exc:
            print(f"[ERROR] No se pudo conectar a MongoDB: {exc}")
            raise

    print("\n" + "═" * 55)
    print("  FASE 3.1 — Transacción ACID Multi-documento")
    print("═" * 55)
    print("""
  Proceso de negocio: suscripción al plan premium.
  Colecciones involucradas:
    1. users        -> cambia plan de "free" a "premium"
    2. subscriptions -> registra la suscripción activa
    3. payments     -> registra el pago asociado
  Si cualquier paso falla -> rollback automático (atomicidad ACID).
""")

    test_user = db.users.find_one({"plan": "free"})
    if not test_user:
        print("  [INFO] No hay usuarios free. Creando uno de prueba...")
        res = db.users.insert_one(
            {
                "username": "demo_user",
                "email": "demo@soundwave.io",
                "plan": "free",
                "created_at": datetime.now(timezone.utc),
            }
        )
        test_user = db.users.find_one({"_id": res.inserted_id})

    print(f"  Usuario seleccionado : {test_user['username']} (plan actual: {test_user['plan']})")
    print(f"  Monto a cobrar       : $9.99 USD")
    print(f"  Método de pago       : credit_card")

    try:
        txn_result = upgrade_to_premium(
            db=db,
            user_id=test_user["_id"],
            amount=9.99,
            payment_method="credit_card",
        )
        print("\n  [OK] Transacción completada exitosamente.")
        mostrar_resultado(db, txn_result)

    except ValueError as exc:
        print(f"  [INFO] {exc}")
    except OperationFailure as exc:
        if exc.code == 20:
            print("""
  [ERROR] Las transacciones ACID requieren un Replica Set.
          Revisá que mongod.cfg tenga la sección replication y reiniciá el servicio MongoDB.
""")
        else:
            traceback.print_exc()
    except Exception:
        traceback.print_exc()


if __name__ == "__main__":
    main()