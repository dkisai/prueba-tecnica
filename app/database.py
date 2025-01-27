import configparser
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Leer configuración
config = configparser.ConfigParser()
config.read("Metrobus_config.ini")

DATABASE_URL = (
    f"postgresql://{config['DATABASE']['user']}:{config['DATABASE']['password']}"
    f"@{config['DATABASE']['host']}:{config['DATABASE']['port']}/{config['DATABASE']['dbname']}"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Dependencia para la sesión
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()