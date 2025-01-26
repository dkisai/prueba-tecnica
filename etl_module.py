from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import StringType
import geopandas as gpd
from shapely import Point
import psycopg2
from google.transit.gtfs_realtime_pb2 import FeedMessage
import requests
import time

# Configuración
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "prueba_akron",
    "user": "dkisai",
    "password": "dkisai"
}

GEOJSON_PATH = "alcaldias/poligonos_alcaldias_cdmx.shp"
SLEEP_INTERVAL = 30  # Intervalo de 30 segundos


# 1. Validar y crear la base de datos
def ensure_database(config: dict) -> None:
    with psycopg2.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"]
    ) as conn:
        conn.set_isolation_level(0)  # AUTOCOMMIT
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{config['dbname']}';")
            if not cursor.fetchone():
                cursor.execute(f"CREATE DATABASE {config['dbname']};")
                print(f"Base de datos '{config['dbname']}' creada.")


# 2. Validar y crear las tablas
def ensure_database(config: dict) -> None:
    # Conectar a la base de datos `postgres` para verificar la existencia de `prueba_akron`
    try:
        with psycopg2.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            dbname="postgres"  # Usar la base de datos predeterminada
        ) as conn:
            conn.set_isolation_level(0)  # Permite comandos de creación de base de datos
            with conn.cursor() as cursor:
                # Verificar si la base de datos existe
                cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{config['dbname']}';")
                if not cursor.fetchone():
                    # Crear la base de datos si no existe
                    cursor.execute(f"CREATE DATABASE {config['dbname']};")
                    print(f"Base de datos '{config['dbname']}' creada exitosamente.")
                else:
                    print(f"La base de datos '{config['dbname']}' ya existe.")
    except psycopg2.OperationalError as e:
        print(f"Error al conectar con PostgreSQL: {e}")
        raise


# 3. Obtener la última URL del feed GTFS
def get_latest_url(config: dict) -> str:
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT url FROM gtfs_links ORDER BY stored_at DESC LIMIT 1;")
            result = cursor.fetchone()
            return result[0] if result else None


# 4. Descargar y procesar el feed GTFS
def download_gtfs_feed(url: str) -> list:
    feed = FeedMessage()
    response = requests.get(url)
    feed.ParseFromString(response.content)

    return [
        (
            entity.id,                                # entity_id
            vehicle.vehicle.id,                      # vehicle_id
            vehicle.trip.route_id,                   # route_id
            vehicle.trip.direction_id,               # direction_id
            vehicle.trip.start_time,                 # start_time
            vehicle.trip.start_date,                 # start_date
            vehicle.vehicle.label,                   # label
            vehicle.vehicle.license_plate,           # license_plate
            vehicle.position.latitude,               # latitude
            vehicle.position.longitude,              # longitude
            vehicle.position.bearing,                # bearing
            vehicle.position.odometer,               # odometer
            vehicle.position.speed,                  # speed
            vehicle.timestamp                        # timestamp
        )
        for entity in feed.entity if (vehicle := entity.vehicle) and vehicle.trip and vehicle.position
    ]


# 5. Cargar los límites de las alcaldías
def load_alcaldias(path: str):
    return gpd.read_file(path)


# 6. Asignar alcaldía a cada vehículo
def get_alcaldia(lat: float, lon: float, alcaldias: gpd.GeoDataFrame) -> str:
    point = Point(lon, lat)
    for _, alcaldia in alcaldias.iterrows():
        if point.within(alcaldia.geometry):
            return alcaldia["NOMGEO"]
    return "No se encontró la alcaldía"


def assign_alcaldias(vehicles: list, alcaldias: gpd.GeoDataFrame) -> list:
    return [
        vehicle + (get_alcaldia(vehicle[8], vehicle[9], alcaldias),)  # Añade alcaldía al final
        for vehicle in vehicles
    ]


# 7. Guardar los datos en PostgreSQL
def save_to_postgres(spark, vehicles: list, config: dict) -> None:
    columns = [
        "entity_id", "vehicle_id", "route_id", "direction_id", "start_time",
        "start_date", "label", "license_plate", "latitude", "longitude",
        "bearing", "odometer", "speed", "timestamp", "alcaldia"
    ]
    vehicles_df = spark.createDataFrame(vehicles, columns)
    vehicles_df = vehicles_df.withColumn("processed_at", current_timestamp())

    vehicles_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{config['host']}:{config['port']}/{config['dbname']}") \
        .option("dbtable", "vehicles_status") \
        .option("user", config["user"]) \
        .option("password", config["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


# 8. Proceso principal
def main():
    # Configurar Spark
    spark = SparkSession.builder \
        .appName("GTFS-Vehicle-Alcaldia") \
        .getOrCreate()

    try:
        # Validar y crear base de datos y tablas
        ensure_database(DB_CONFIG)

        # Cargar alcaldías
        alcaldias = load_alcaldias(GEOJSON_PATH)

        while True:
            # Obtener la última URL
            url = get_latest_url(DB_CONFIG)
            if not url:
                print("No se encontró una URL válida.")
                time.sleep(SLEEP_INTERVAL)
                continue

            # Descargar y procesar el feed
            vehicles = download_gtfs_feed(url)

            # Asignar alcaldías
            vehicles_with_alcaldias = assign_alcaldias(vehicles, alcaldias)

            # Guardar en PostgreSQL
            save_to_postgres(spark, vehicles_with_alcaldias, DB_CONFIG)

            # Esperar antes del próximo ciclo
            time.sleep(SLEEP_INTERVAL)

    except KeyboardInterrupt:
        print("Proceso detenido.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()