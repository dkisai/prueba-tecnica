import logging
import re
import imaplib
import email
from email.policy import default
import requests
from typing import Optional, Dict, List
import psycopg2

logger = logging.getLogger(__name__)

def obtener_token(correo: str) -> Optional[Dict]:
    """
    Solicita a la API el token asociado a 'correo'.
    """
    base_url: str = "https://metrobus-gtfs.sinopticoplus.com/gtfs-api"
    endpoint: str = f"{base_url}/validateEmailMetrobus/{correo}"
    
    try:
        resp = requests.get(endpoint, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        if data and data != "not registered":
            logger.info("Token obtenido correctamente para el correo: %s", correo)
            return data
        
        logger.warning("El correo %s no está registrado en la API.", correo)
        return None
    except requests.exceptions.RequestException as e:
        logger.error("Error al obtener token desde %s: %s", endpoint, e)
        return None

def enviar_correo_gtfs(correo: str, token_data: Dict) -> bool:
    """
    Envía la solicitud de correo GTFS a la API.
    
    :param correo: El correo electrónico destino.
    :param token_data: Diccionario que contiene al menos la clave 'token'.
    :return: True si se envía con éxito, False en caso contrario.
    """
    if "token" not in token_data:
        logger.error("El diccionario de token_data no contiene la clave 'token'.")
        return False
    
    url: str = f"https://metrobus-gtfs.sinopticoplus.com/gtfs-api/senderEmailGtfs/1339/{correo}"
    headers: Dict[str, str] = {
        "Authorization": f"Bearer {token_data['token']}",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0",
        "Origin": "https://metrobus-gtfs.sinopticoplus.com",
        "Referer": "https://metrobus-gtfs.sinopticoplus.com/static/sendEmail"
    }
    
    try:
        response = requests.post(url, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info("Correo GTFS enviado con éxito. Status Code: %s", response.status_code)
        return True
    except requests.exceptions.RequestException as e:
        logger.error("Error al enviar correo GTFS: %s", e)
        return False

def _recorrer_partes_mensaje(msg: email.message.EmailMessage) -> List[str]:
    """
    Función recursiva para extraer el contenido de todas las partes de
    un mensaje multipart.
    """
    contenidos: List[str] = []
    
    if msg.is_multipart():
        for part in msg.iter_parts():
            contenidos.extend(_recorrer_partes_mensaje(part))
    else:
        content_type: str = msg.get_content_type()
        if content_type in ("text/plain", "text/html"):
            contenidos.append(msg.get_content())
    
    return contenidos

def obtener_link_gtfs(imap_server: str, correo: str, password: str) -> Optional[str]:
    """
    Conecta a Gmail por IMAP, busca el último correo en INBOX y extrae 
    el link que incluya 'Metrobus_GTFS_RT.proto'.
    """
    try:
        mail = imaplib.IMAP4_SSL(imap_server)
        mail.login(correo, password)
        mail.select("INBOX")
        
        status, data = mail.search(None, "ALL")
        if status != "OK":
            logger.error("No se pudo buscar en el buzón. Status: %s", status)
            return None
        
        email_ids = data[0].split()
        if not email_ids:
            logger.warning("No hay correos en la carpeta.")
            return None
        
        latest_email_id = email_ids[-1]
        status, msg_data = mail.fetch(latest_email_id, "(RFC822)")
        
        if status != "OK":
            logger.error("No se pudo obtener el contenido del correo. Status: %s", status)
            return None
        
        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email, policy=default)
        
        partes_mensaje: List[str] = _recorrer_partes_mensaje(msg)
        body: str = "\n".join(partes_mensaje)
        
        links = re.findall(r'href="([^"]*Metrobus_GTFS_RT\.proto[^"]*)"', body)
        if not links:
            logger.warning("No se encontraron enlaces en el correo.")
            return None
        
        logger.info("Link GTFS encontrado en el correo.")
        return links[0]
    except imaplib.IMAP4.error as e:
        logger.error("Error IMAP al obtener el link GTFS: %s", e)
    except Exception as e:
        logger.error("Error desconocido al obtener el link GTFS: %s", e)
    
    return None

def almacenar_link_en_bd(
    host: str, port: str, dbname: str, user: str, password: str, link: str
) -> bool:
    """
    Inserta la URL en la tabla 'gtfs_links' (creándola si no existe),
    junto con la fecha/hora actual (NOW()).
    
    :return: True si se insertó con éxito, False en caso de error.
    """
    conn_str = (
        f"dbname={dbname} user={user} password={password} host={host} port={port}"
    )
    
    try:
        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                # 1) Crear la tabla si no existe
                create_table_sql = """
                    CREATE TABLE IF NOT EXISTS gtfs_links (
                        id SERIAL PRIMARY KEY,
                        url TEXT NOT NULL,
                        stored_at TIMESTAMP NOT NULL DEFAULT NOW()
                    )
                """
                cursor.execute(create_table_sql)
                
                # 2) Insertar el link en la tabla
                insert_sql = """INSERT INTO gtfs_links (url) VALUES (%s)"""
                cursor.execute(insert_sql, (link,))
                
        logger.info("URL '%s' almacenada en la tabla 'gtfs_links' exitosamente.", link)
        return True
    except psycopg2.Error as e:
        logger.error("Error al insertar la URL en la base de datos: %s", e)
        return False