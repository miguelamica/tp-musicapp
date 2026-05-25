# SoundWave - TP Integrador NoSQL

Guia corta para ejecutar el proyecto en Windows.

## Requisitos

- MongoDB instalado y en ejecucion
- MongoDB Shell (`mongosh`) instalado
- Redis instalado y en ejecucion
- Python 3

## Instalacion de dependencias

Desde la carpeta `tp-musicapp`:

```powershell
pip install -r requirements.txt
```

## Preparar MongoDB para transacciones

La fase 3 usa transacciones ACID, asi que MongoDB debe estar corriendo como **replica set**.

1. Verificá que MongoDB esté iniciado.
2. Abrí `mongosh`.
3. Ejecutá una sola vez:

```javascript
rs.initiate()
```

Si querés comprobar que quedó listo:

```javascript
rs.status()
```

## Preparar Redis

Iniciá el servidor de Redis antes de ejecutar la fase 4.

## Ejecutar el proyecto

Desde la carpeta `tp-musicapp`:

```powershell
python main.py
```

Eso ejecuta todas las fases en orden.

También podés ejecutar una fase puntual:

```powershell
python main.py --fase 1   # seed
python main.py --fase 2   # aggregation + indices
python main.py --fase 3   # transacciones ACID + CAP + sharding
python main.py --fase 4   # Redis
```

## Notas

- La conexion por defecto usa `mongodb://localhost:27017/`.
- Redis usa `localhost:6379`.
- Si la fase 3 falla con un error de transacciones, revisá que MongoDB esté iniciado como replica set.
