# Proyecto Metrobus - FastAPI + Airflow + Postgres

Este proyecto tiene como objetivo:

1. **Proveer una API (FastAPI)** para consultar alcaldías, paradas y estatus de vehículos (Metrobus) desde una base de datos PostgreSQL.  
2. **Orquestar tareas ETL** con **Airflow**, almacenando y procesando datos.  
3. **Dockerizar** todo el ecosistema para facilitar la puesta en marcha (un contenedor con la app FastAPI y la lógica ETL, un contenedor de Postgres, y otro de Airflow).

## Tabla de Contenidos

- [Instalación y Requerimientos](#instalación-y-requerimientos)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Configuraciones](#configuraciones)
- [Ejecución con Docker Compose](#ejecución-con-docker-compose)
- [Uso de la API](#uso-de-la-api)
- [Ejecutar Procesos ETL (opcional con PySpark local)](#ejecutar-procesos-etl-opcional-con-pyspark-local)
- [Orquestación con Airflow](#orquestación-con-airflow)
  
---

## Instalación y Requerimientos

- **Docker** y **Docker Compose** instalados en tu sistema.
- **Python 3.10+** si deseas probar partes del proyecto localmente.
- Conexión a internet para descargar imágenes y dependencias.

---

## Estructura del Proyecto

```bash
.
├── app/
│   ├── __init__.py
│   ├── main.py               
│   ├── database.py           
│   ├── models.py             
│   ├── routes.py            
│   ├── etl_module.py        
│   └── ...
├── dags/
│   └── dag_db_setup.py
│   └── dag_etl_module.py
│   └── dag_url_module.py             
├── Metrobus_config.ini       
├── Dockerfile               
├── docker-compose.yaml      
├── requirements.txt          
└── README.md                
```

---

## Configuraciones

- **Metrobus_config.ini**: Contiene variables para la conexión a la base de datos (host, port, dbname, user, password).  
- **docker-compose.yaml**: Define servicios:
  - `db` (Postgres)
  - `airflow` (scheduler y webserver)
  - `fastapi` (nuestra aplicación principal con PySpark instalado en modo local, si requieres ETL)

---

## Ejecución con Docker Compose

1. **Clonar** el repositorio:
   ```bash
   git clone https://github.com/tu-usuario/mi-proyecto.git
   cd mi-proyecto
   ```
2. (Opcional) **Ajustar `Metrobus_config.ini`** o variables en `docker-compose.yaml` (usuario/contraseña de DB).
3. **Construir e iniciar** los contenedores:
   ```bash
   docker-compose up --build
   ```
4. Esperar a que todos los servicios arranquen:
   - **Postgres** en `localhost:5432`
   - **Airflow** en `localhost:8080` (usuario: `admin`, password: `admin`, si sigues el ejemplo)
   - **FastAPI** en `localhost:8000`
5. (Opcional) Para parar los contenedores:
   ```bash
   docker-compose down
   ```

---

## Uso de la API

Una vez que los contenedores estén en marcha:

- Visita `http://localhost:8000/docs` para ver la documentación interactiva (Swagger) de **FastAPI**.

Dependiendo de tu `routes.py`, podrías tener endpoints como:

1. **Listar unidades** con ruta asignada:
   ```
   GET /unidades/
   ```
2. **Consultar ubicación de un vehículo**:
   ```
   GET /unidades/{vehicle_id}/ubicaciones
   ```
3. **Listar alcaldías** (en `vehicles_status`):
   ```
   GET /alcaldias/
   ```
4. **Paradas según alcaldía**:
   ```
   GET /paradas/{alcaldia_nombre}
   ```

---

## Ejecutar Procesos ETL (opcional con PySpark local)

Si tu módulo `etl_module.py` usa PySpark, se ejecutaría en **modo local** dentro del contenedor `fastapi`. Ejemplo de función:

```python
# app/etl_module.py
from pyspark.sql import SparkSession

def run_etl():
    spark = SparkSession.builder \
        .appName("MetrobusETL") \
        .master("local[*]") \
        .getOrCreate()

    # Realiza tus transformaciones...
    spark.stop()
```

Para lanzarlo de forma manual:

1. Ingresa al contenedor:
   ```bash
   docker exec -it fastapi_app bash
   ```
2. Ejecuta tu script:
   ```bash
   python app/etl_module.py
   ```
   *(Asumiendo que tu `etl_module.py` es ejecutable como un script.)*

Si deseas **orquestarlo** con Airflow, ve a la siguiente sección.

---

## Orquestación con Airflow

Al habilitar Airflow en `docker-compose.yaml`, tendrás:

1. **Airflow Webserver** en `http://localhost:8080`
2. **DAGs** montados desde la carpeta `dags/` para que Airflow los reconozca.

---

### Notas Finales

- Revisa la documentación de [FastAPI](https://fastapi.tiangolo.com/) para funcionalidades más avanzadas.  
- [Apache Airflow](https://airflow.apache.org/) para orquestación.  
- [PySpark](https://spark.apache.org/docs/latest/api/python/) si manejas grandes volúmenes de datos en modo local.  

"""
