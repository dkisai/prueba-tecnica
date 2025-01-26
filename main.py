# main.py
import logging

from etl_module import run_etl_process
from url_module import run_url_process

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)

def main():
    # Ejemplo: primero ejecutamos url_module, luego etl_module
    run_url_process("metrobus_config.ini")
    run_etl_process("metrobus_config.ini")

if __name__ == "__main__":
    main()