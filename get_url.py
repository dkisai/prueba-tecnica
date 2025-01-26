import imaplib
import email
from email.policy import default
import re
import requests

IMAP_SERVER = "imap.gmail.com"

def obtener_token(email: str) -> str | None:
    base_url = "https://metrobus-gtfs.sinopticoplus.com/gtfs-api"
    resp = requests.get(f"{base_url}/validateEmailMetrobus/{email}")
    if resp.status_code == 200:
        data = resp.json()
        if data and data != "not registered":
            return data  # Este es el token
    return None

def enviar_correo_gtfs(email: str, token: str) -> None:
    # URL exacta para el envío de correo
    url = f"https://metrobus-gtfs.sinopticoplus.com/gtfs-api/senderEmailGtfs/1339/{email}"
    
    # Cabeceras capturadas de tu petición. Ajusta según sea necesario.
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0",
        "Origin": "https://metrobus-gtfs.sinopticoplus.com",
        "Referer": "https://metrobus-gtfs.sinopticoplus.com/static/sendEmail"
    }

    # Realizas la petición POST.
    response = requests.post(url, headers=headers)

    print("Status Code:", response.status_code)
    print("Response Body:", response.text)

def obtener_link_gtfs(correo: str, password: str) -> str | None:
    """
    Conecta a Gmail por IMAP, busca el último correo (o uno filtrado) 
    y extrae el link que necesitas.
    Retorna el enlace como string, o None si no lo encuentra.
    """
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(correo, password)
    mail.select("INBOX")

    status, data = mail.search(None, "ALL")  
    if status != "OK":
        print("No se pudo buscar en el buzón.")
        return None

    email_ids = data[0].split()
    if not email_ids:
        print("No hay correos en la carpeta.")
        return None
    latest_email_id = email_ids[-1]

    status, msg_data = mail.fetch(latest_email_id, "(RFC822)")
    if status != "OK":
        print("No se pudo obtener el contenido del correo.")
        return None

    raw_email = msg_data[0][1]
    msg = email.message_from_bytes(raw_email, policy=default)

    mail_content = []
    if msg.is_multipart():
        for part in msg.iter_parts():
            content_type = part.get_content_type()
            if content_type in ("text/plain", "text/html"):
                mail_content.append(part.get_content())
    else:
        mail_content.append(msg.get_content())

    # Unir todo el contenido en un solo string
    body = "\n".join(mail_content)

    # Buscar el enlace con una expresión regular
    links = re.findall(r'href="([^"]*Metrobus_GTFS_RT\.proto[^"]*)"', body)

    if not links:
        print("No se encontraron enlaces en el correo.")
        return None

    return links[0]

if __name__ == "__main__":
    correo:str = "test.dkisai@gmail.com"
    password:str = "kdxi jiex pivh rbhq"  
    token:str = obtener_token(correo)
    enviar_correo_gtfs(correo, token["token"])

    link_gtfs = obtener_link_gtfs(correo)
    if link_gtfs:
        print("Link GTFS encontrado:", link_gtfs)
    else:
        print("No se pudo encontrar el link en el correo.")