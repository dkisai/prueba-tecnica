from fastapi import FastAPI
from .database import Base, engine
from .routes import router

# Crear las tablas (opcional, aunque muchos prefieren migraciones con Alembic)
Base.metadata.create_all(bind=engine)

# Crear instancia de FastAPI
app = FastAPI()

# Incluir las rutas
app.include_router(router)
