import logging
import configparser
from typing import Optional, Dict

from url_module import (
    obtener_token,
    enviar_correo_gtfs,
    obtener_link_gtfs,
    almacenar_link_en_bd
)

logging.basicConfig(
    filename='app.log', 
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main() -> None:
    """
    Flujo principal:
    1. Carga la config (correo, IMAP, DB)
    2. Obtiene token
    3. Envía correo GTFS
    4. Obtiene link del correo
    5. Crea/actualiza tabla y guarda URL
    """
    config = configparser.ConfigParser()
    config.read("metrobus_config.ini")
    
    imap_server: str = config["DEFAULT"].get("imap_server", "imap.gmail.com")
    correo: str = config["DEFAULT"].get("correo", "")
    password_correo: str = config["DEFAULT"].get("password", "")
    
    host: str = config["DATABASE"].get("host", "localhost")
    port: str = config["DATABASE"].get("port", "5432")
    dbname: str = config["DATABASE"].get("dbname", "")
    user_db: str = config["DATABASE"].get("user", "")
    password_db: str = config["DATABASE"].get("password", "")
    
    # Validamos datos
    if not correo or not password_correo:
        logger.error("No se encontró correo/contraseña en la configuración.")
        return
    if not dbname or not user_db:
        logger.error("No se encontró info de base de datos en la configuración.")
        return
    
    token_data: Optional[Dict] = obtener_token(correo)
    if not token_data:
        logger.error("No se pudo obtener el token.")
        return
    
    if not enviar_correo_gtfs(correo, token_data):
        logger.error("No se pudo enviar el correo GTFS.")
        return
    
    link_gtfs: Optional[str] = obtener_link_gtfs(imap_server, correo, password_correo)
    if not link_gtfs:
        logger.error("No se pudo obtener el link del correo.")
        return
    
    if almacenar_link_en_bd(host, port, dbname, user_db, password_db, link_gtfs):
        logger.info("Proceso completado: link almacenado correctamente.")
    else:
        logger.error("Ocurrió un error al almacenar el link en la BD.")

if __name__ == "__main__":
    main()