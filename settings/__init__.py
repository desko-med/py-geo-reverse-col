import os
import sys
import logging

from datetime import datetime


class AppBaseSettings:
    """
    Variables globales con la configuracion de las rutas, el logger, entre otras.
    """
    APP_NAME: str = "Geo Reverse Col"
    APP_VERSION: str = '0.0.0'

    APP_RELPATH: str = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    APP_MODEL_PATH: str = f'{APP_RELPATH}\\model_data\\model\\' \
        if os.name == 'nt' else f'{APP_RELPATH}/model_data/model/'
    APP_MODEL_DATA_PATH: str = f'{APP_RELPATH}\\model_data\\data\\' \
        if os.name == 'nt' else f'{APP_RELPATH}/model_data/data/'
    APP_MODEL_RESULTS_PATH: str = f'{APP_RELPATH}\\model_data\\results\\' \
        if os.name == 'nt' else f'{APP_RELPATH}/model_data/results/'
    APP_MODEL_CATALOG_PATH: str = f'{APP_RELPATH}\\catalog\\' if os.name == 'nt' else f'{APP_RELPATH}/catalog/'
    APP_GEOJSON_CATALOG_PATH: str = f'{APP_RELPATH}\\geojson\\' if os.name == 'nt' else f'{APP_RELPATH}/geojson/'

    # Variables de Tiempo:
    TIME_NODASH = datetime.today().strftime("%Y%m%d%H%M%S")
    TIME_DATE_TODAY = datetime.today().strftime("%Y-%m-%d")

    # Configuracion del Logger
    logger_app_name = APP_NAME
    logger = logging.getLogger(logger_app_name)
    logger.setLevel(logging.INFO)
    consoleHandle = logging.StreamHandler(sys.stdout)
    consoleHandle.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    consoleHandle.setFormatter(formatter)
    logger.addHandler(consoleHandle)


class AppSettings(AppBaseSettings):
    pass


Settings = AppSettings()
