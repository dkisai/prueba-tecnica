# Usamos una imagen base ligera de Python
FROM python:3.10-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos el archivo de requisitos
COPY requirements.txt requirements.txt

# Instalamos dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto del código (carpeta app/) y el archivo .ini si lo usas
COPY app/ app/
COPY Metrobus_config.ini Metrobus_config.ini

# Exponemos el puerto donde correrá FastAPI
EXPOSE 8000

# Comando por defecto: levantar uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]