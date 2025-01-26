# etl_module.py
from __future__ import annotations
import logging
import configparser
import os
import re
import time
from datetime import datetime, timedelta

# Dependencias ETL
import psycopg2
import requests
import geopandas as gpd
from shapely.geometry import Point
from unidecode import unidecode

from google.transit.gtfs_realtime_pb2 import FeedMessage
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Configuramos logger para este módulo
logger = logging.getLogger(__name__)

def run_etl_process(config_path: str = "metrobus_config.ini") -> None:
    """
    Ejecuta el proceso ETL completo:
      1. Valida/crea DB y tablas
      2. Obtiene URL más reciente (menos de 11h)
      3. Descarga feed GTFS
      4. Asigna alcaldías con GeoPandas
      5. Inserta/actualiza datos en PostgreSQL (UPSERT)
    """

    # 1) Leemos la configuración desde 'metrobus_config.ini'
    cfg = configparser.ConfigParser()
    cfg.read(config_path)

    # Extraemos DB_CONFIG desde la sección [DATABASE]
    db_config = {
        "host": cfg["DATABASE"].get("host", "localhost"),
        "port": cfg["DATABASE"].getint("port", 5432),
        "dbname": cfg["DATABASE"].get("dbname", "prueba_akron"),
        "user": cfg["DATABASE"].get("user", "dkisai"),
        "password": cfg["DATABASE"].get("password", "dkisai"),
    }
    
    # Paths y parámetros adicionales, si quieres ponerlos en [DEFAULT] o [ETL]
    JDBC_DRIVER_PATH = cfg["DEFAULT"].get("jdbc_driver_path", "postgresql-42.7.5.jar")
    GEOJSON_PATH = cfg["DEFAULT"].get("geojson_path", "alcaldias/poligonos_alcaldias_cdmx.shp")

    # 2) Validamos base de datos y tablas (ver funciones abajo)
    ensure_database(db_config)
    ensure_tables(db_config)

    # 3) Iniciamos Spark
    spark = SparkSession.builder \
        .appName("GTFS-Vehicle-Alcaldia") \
        .config("spark.sql.encoding", "UTF-8") \
        .config("spark.executorEnv.LANG", "es_MX.UTF-8") \
        .config("spark.jars", JDBC_DRIVER_PATH) \
        .getOrCreate()

    try:
        # 4) Cargamos los límites de las alcaldías
        alcaldias = load_alcaldias(GEOJSON_PATH)

        # 5) Obtenemos la última URL (<= 11 horas)
        url = get_latest_url(db_config)
        if not url:
            logger.warning("No se encontró una URL válida en 'gtfs_links'.")
            return

        # 6) Descargamos y procesamos el feed GTFS
        vehicles = download_gtfs_feed(url)
        logger.info("Se obtuvieron %d registros de vehículos.", len(vehicles))

        # 7) Asignar alcaldías
        vehicles_with_alcaldias = assign_alcaldias(vehicles, alcaldias)
        logger.info("Se asignaron alcaldías a los vehículos.")

        # 8) Guardar datos en PostgreSQL con upsert
        save_to_postgres_with_upsert(spark, vehicles_with_alcaldias, db_config)
        logger.info("Datos procesados y guardados exitosamente.")

    finally:
        spark.stop()
        logger.info("Spark finalizado.")

### FUNCIONES AUXILIARES ###

def ensure_database(config: dict) -> None:
    """
    Valida y crea la base de datos si no existe.
    """
    logger.info("Validando existencia de la base de datos '%s'.", config['dbname'])
    # Nos conectamos a la DB 'postgres' para crear la DB si no existe
    tmp_dbname = config["dbname"]
    config_for_postgres = {**config, "dbname": "postgres"}
    with psycopg2.connect(**config_for_postgres) as conn:
        conn.set_isolation_level(0)
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = %s;", (tmp_dbname,))
            if not cursor.fetchone():
                cursor.execute(f"CREATE DATABASE {tmp_dbname};")
                logger.info("Base de datos '%s' creada.", tmp_dbname)
            else:
                logger.info("La base de datos '%s' ya existe.", tmp_dbname)

def ensure_tables(config: dict) -> None:
    """
    Valida y crea las tablas necesarias si no existen.
    """
    logger.info("Verificando tablas necesarias (gtfs_links, vehicles_status).")
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS gtfs_links (
                    id SERIAL PRIMARY KEY,
                    url TEXT NOT NULL,
                    stored_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vehicles_status (
                    id SERIAL PRIMARY KEY,
                    vehicle_id TEXT UNIQUE,
                    entity_id TEXT,
                    route_id TEXT,
                    start_time TEXT,
                    start_date TEXT,
                    label TEXT,
                    license_plate TEXT,
                    latitude FLOAT,
                    longitude FLOAT,
                    alcaldia TEXT,
                    odometer FLOAT,
                    speed FLOAT,
                    timestamp BIGINT,
                    processed_at TIMESTAMP
                );
            """)
    logger.info("Tablas validadas/creadas.")

def get_latest_url(config: dict) -> str | None:
    """
    Obtiene la última URL registrada en gtfs_links,
    validando que tenga menos de 11 horas.
    """
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT url, stored_at FROM gtfs_links ORDER BY stored_at DESC LIMIT 1;")
            result = cursor.fetchone()

            if result:
                url, stored_at = result
                # Verificar si la URL tiene menos de 11 horas
                time_diff = datetime.now() - stored_at
                if time_diff > timedelta(hours=11):
                    msg = "El link es demasiado viejo (más de 11 horas)."
                    logger.error(msg)
                    raise ValueError(msg)
                return url
            else:
                logger.error("No se encontró ninguna URL en 'gtfs_links'.")
                return None

def download_gtfs_feed(url: str) -> list[tuple]:
    """
    Descarga y parsea el feed GTFS-RT.
    Retorna una lista de tuplas con la información de cada vehículo.
    """
    logger.info("Descargando feed GTFS desde: %s", url)
    feed = FeedMessage()
    response = requests.get(url)
    response.raise_for_status()
    feed.ParseFromString(response.content)

    vehicles = []
    for entity in feed.entity:
        if not entity.vehicle or not entity.vehicle.trip or not entity.vehicle.position:
            continue
        vehicle = entity.vehicle
        vehicles.append((
            entity.id,
            vehicle.vehicle.id,
            vehicle.trip.route_id,
            getattr(vehicle.trip, 'direction_id', None),
            vehicle.trip.start_time,
            vehicle.trip.start_date,
            vehicle.vehicle.label,
            vehicle.vehicle.license_plate,
            vehicle.position.latitude,
            vehicle.position.longitude,
            getattr(vehicle.position, 'bearing', None),
            getattr(vehicle.position, 'odometer', None),
            getattr(vehicle.position, 'speed', None),
            vehicle.timestamp
        ))
    return vehicles

def load_alcaldias(path: str) -> gpd.GeoDataFrame:
    """
    Carga el shapefile/GeoJSON de alcaldías con GeoPandas.
    """
    logger.info("Cargando límites de alcaldías desde: %s", path)
    alcaldias = gpd.read_file(path, encoding="utf-8")
    alcaldias["NOMGEO"] = alcaldias["NOMGEO"].apply(normalize_text)
    return alcaldias

def normalize_text(text: str) -> str:
    """
    Normaliza un texto: elimina acentos, signos de puntuación, etc.
    """
    text = unidecode(text.lower().strip())
    text = re.sub(r"[^\w\s]", "", text)  # elimina signos de puntuación
    text = re.sub(r"\s+", " ", text)     # compacta espacios
    return text

def get_alcaldia(lat: float, lon: float, alcaldias: gpd.GeoDataFrame) -> str:
    """
    Determina a qué alcaldía pertenece la coordenada (lat, lon).
    """
    point = Point(lon, lat)
    for _, alcaldia in alcaldias.iterrows():
        if point.within(alcaldia.geometry):
            return normalize_text(alcaldia["NOMGEO"])
    return "no_se_encontro_la_alcaldia"

def assign_alcaldias(vehicles: list[tuple], alcaldias: gpd.GeoDataFrame) -> list[tuple]:
    """
    Asigna alcaldía a cada vehículo.
    Se asume que vehicles[i] = (
      entity_id, vehicle_id, route_id, direction_id, start_time, start_date, label, license_plate,
      latitude, longitude, bearing, odometer, speed, timestamp
    )
    Retornamos la misma tupla + alcaldia al final.
    """
    output = []
    for v in vehicles:
        lat, lon = v[8], v[9]
        al = get_alcaldia(lat, lon, alcaldias)
        output.append(v + (al,))
    return output

def save_to_postgres_with_upsert(spark, vehicles: list[tuple], config: dict) -> None:
    """
    Guarda los datos en la tabla 'vehicles_status' con estrategia UPSERT.
    """
    logger.info("Guardando datos en 'vehicles_status' con UPSERT.")
    columns = [
        "entity_id", "vehicle_id", "route_id", "direction_id", "start_time",
        "start_date", "label", "license_plate", "latitude", "longitude",
        "bearing", "odometer", "speed", "timestamp", "alcaldia"
    ]
    
    # Creamos DF PySpark
    vehicles_df = spark.createDataFrame(vehicles, columns)
    vehicles_df = vehicles_df.withColumn("processed_at", current_timestamp())

    # Convertimos a Pandas
    vehicles_pd = vehicles_df.toPandas()

    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    upsert_query = """
    INSERT INTO vehicles_status (
        entity_id, vehicle_id, route_id, start_time, start_date,
        label, license_plate, latitude, longitude, odometer, speed,
        timestamp, alcaldia, processed_at
    )
    VALUES (
        %(entity_id)s, %(vehicle_id)s, %(route_id)s, %(start_time)s, %(start_date)s,
        %(label)s, %(license_plate)s, %(latitude)s, %(longitude)s, %(odometer)s, %(speed)s,
        %(timestamp)s, %(alcaldia)s, %(processed_at)s
    )
    ON CONFLICT (vehicle_id)
    DO UPDATE SET
        entity_id = EXCLUDED.entity_id,
        route_id = EXCLUDED.route_id,
        start_time = EXCLUDED.start_time,
        start_date = EXCLUDED.start_date,
        label = EXCLUDED.label,
        license_plate = EXCLUDED.license_plate,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        odometer = EXCLUDED.odometer,
        speed = EXCLUDED.speed,
        timestamp = EXCLUDED.timestamp,
        alcaldia = EXCLUDED.alcaldia,
        processed_at = EXCLUDED.processed_at;
    """

    for _, row in vehicles_pd.iterrows():
        # row.to_dict() regresa un dict con las columnas del DataFrame
        params = {
            "entity_id":       row["entity_id"],
            "vehicle_id":      row["vehicle_id"],
            "route_id":        row["route_id"],
            "start_time":      row["start_time"],
            "start_date":      row["start_date"],
            "label":           row["label"],
            "license_plate":   row["license_plate"],
            "latitude":        row["latitude"],
            "longitude":       row["longitude"],
            "odometer":        row["odometer"],
            "speed":           row["speed"],
            "timestamp":       row["timestamp"],
            "alcaldia":        row["alcaldia"],
            "processed_at":    row["processed_at"]
        }
        cursor.execute(upsert_query, params)

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("UPSERT completado en 'vehicles_status'.")