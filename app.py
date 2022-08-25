"""
Geo Reverse Colombia
Proyecto para hacer geo reverse y determinar los sectores, ciudades, departamentos, paises
dependiendo de las coordenadas (LAT, LONG)
"""

import time
import os
import yaml
import csv
import geopip
import pandas as pd

from datetime import datetime
from settings import Settings
from dataobs import DataObs
from utils import list_files_in_dir, create_unique_file_name


class GeoReversePipeline:

    def __init__(self, geo_list: list = None):
        self.current_time = time.time()
        self.model_config: dict = {}
        self.geo_config: dict = {}
        self.geo_list = geo_list
        self.batch_insert: list = []
        self.model_results_path = None
        self.model_predictions = 0
        self.model_predictions_failure = 0
        self.df_data = None
        self.df_result = []
        self.ds = datetime.today()

        # Data Obs: Tracking ------------------------------------------------------------
        self.dataobs_event = {
            'app_country': 'colombia',
            'app_name': Settings.APP_NAME,
            'app_version': Settings.APP_VERSION,
            'app_status_code': 200,
            'app_status_desc': 'success',
            'app_run_ts': self.current_time,
            'app_runtime': None,
            'app_payload': {
                'model_name': 'GeoPip',
                'model_predictions': self.model_predictions_failure,
                'model_predictions_failure': self.model_predictions_failure
            }
        }
        # Data Obs: Logging ------------------------------------------------------------
        msg = f'Iniciando proceso'
        DataObs.print_log(log_type='info', step=self.__class__.__name__, msg=msg)

    def load_config(self):
        """
        Load static variables from the catalog that include aws settings with the required paths

        aws-settings: use s3://bucket-name/prefix/ (not a single file, use folder instead)
            s3-data-key: S3 key where the new dataset is stored \n
            s3-output-key: S3 key where the predictions will be uploaded
        """

        # Data Obs: Logging ------------------------------------------------------------
        msg = f'load_model_catalog: Cargando configuracion'
        DataObs.print_log(log_type='info', step=self.__class__.__name__, msg=msg)

        try:
            config_path = f'{Settings.APP_MODEL_CATALOG_PATH}config.yml'
            with open(config_path, 'r') as yml:
                self.model_config = yaml.safe_load(yml)
        except Exception as e:
            self.dataobs_event.update({
                'app_status_code': 401,
                'app_status_desc': 'Error cargando el catalog'
            })
            msg = f"{self.dataobs_event['app_status_desc']} - Exception: {e}"
            DataObs.print_log(log_type='error', step=f"{self.__class__.__name__}", msg=msg)
            raise

    def load_config_geomaps(self):
        """
        Load geojson config from geojson path.
        metadatos.yml: contains the columns to be selected from geojson
        data.geo.json: geojson with the properties of the maps.
        """

        # Iteramos por cada una de las carpetas y creamos un diccionario con todas las configuraciones
        for folder in os.listdir(Settings.APP_GEOJSON_CATALOG_PATH):
            for catalog in os.listdir(Settings.APP_GEOJSON_CATALOG_PATH + folder):

                # Si mandaron parametro de mapas, excluimos los que no estan
                if self.geo_list and catalog not in self.geo_list:
                    continue

                metadata_path = f'{Settings.APP_GEOJSON_CATALOG_PATH}{folder}/{catalog}/metadatos.yml'
                geojson_path = f'{Settings.APP_GEOJSON_CATALOG_PATH}{folder}/{catalog}/data.geo.json'

                with open(metadata_path, 'r') as stream:
                    metadata = yaml.safe_load(stream)

                metadata_keys = list(metadata['properties'].keys())
                keys_alias = [metadata['properties'][col]['alias'] for col in metadata_keys]
                geojson_columns = dict(zip(metadata_keys, keys_alias))

                self.geo_config.update({
                    catalog: {
                        'geojson_columns': geojson_columns,
                        'geojson_path': geojson_path
                    }
                })

    @staticmethod
    def calcultate_geohash(lat=None, long=None, pre=12):
        __base32 = '0123456789bcdefghjkmnpqrstuvwxyz'
        __decodemap = {}

        lat = float(lat)
        long = float(long)

        for i in range(len(__base32)):
            __decodemap[__base32[i]] = i
        del i
        precision = pre
        lat_interval, lon_interval = (-90.0, 90.0), (-180.0, 180.0)
        geohash = []
        bits = [16, 8, 4, 2, 1]
        bit = 0
        ch = 0
        even = True
        while len(geohash) < precision:
            if even:
                mid = (lon_interval[0] + lon_interval[1]) / 2
                if long > mid:
                    ch |= bits[bit]
                    lon_interval = (mid, lon_interval[1])
                else:
                    lon_interval = (lon_interval[0], mid)
            else:
                mid = (lat_interval[0] + lat_interval[1]) / 2
                if lat > mid:
                    ch |= bits[bit]
                    lat_interval = (mid, lat_interval[1])
                else:
                    lat_interval = (lat_interval[0], mid)
            even = not even
            if bit < 4:
                bit += 1
            else:
                geohash += __base32[ch]
                bit = 0
                ch = 0
        return ''.join(geohash)

    @staticmethod
    def do_georeverse_geopip(lat=None, long=None, columns=None, geopip_=None):
        """
        Funcion para usar GEOPIP y hacer el geo-reverse dado una latitud y longitud
        https://github.com/tammoippen/geopip#search
        :param lat: latitud
        :param long: longitud
        :param columns: columnas o propiedades que vamos a extraer del geojson
        :param geopip_: Instancia de GeoPip segun el mapa a usar
        :return: Dict[Any, Any]. '-1' si result no trajo info
        """
        lat = float(lat)
        long = float(long)
        result_set = {}
        results = geopip_.search(lng=long, lat=lat)

        if results:
            for colu in columns:
                result_set.update({
                    columns[colu]: results.get(colu, '-1')
                })
        else:
            for colu in columns:
                result_set.update({
                    columns[colu]: '-1'
                })

        return result_set

    def load_maps(self):
        """
        Funcion para instanciar en memoria la libreria Geopip segun el mapa seleccionado.
        Se actualiza en la variable geo_config.
        https://github.com/tammoippen/geopip#geopip
        """
        # Data Obs: Logging ------------------------------------------------------------
        msg = f'load_maps: Cargando mapas...'
        DataObs.print_log(log_type='info', step=self.__class__.__name__, msg=msg)

        # Iteramos por los geojson e instanciamos GeoPip con su respectiva configuracion
        for item in self.geo_config:
            if isinstance(self.geo_config[item], dict):
                world_map_ = self.geo_config[item]['geojson_path']
                self.geo_config[item][f'geopip'] = geopip.GeoPIP(filename=world_map_)

    def row_iterator(self):
        """
        Funcion para iterar por cada uno de los registros de los archivos y hacer el geo-reverse
        """
        self.df_result = []
        model_data_key = self.model_config['aws-settings']['s3-data-key']
        model_data_path = f'{Settings.APP_MODEL_DATA_PATH}{model_data_key[model_data_key.rfind("/") + 1:]}'

        # Data Obs: Logging ------------------------------------------------------------
        msg = f'row_iterator: Iniciando predicciones...'
        DataObs.print_log(log_type='info', step=self.__class__.__name__, msg=msg)

        # output_key = self.model_config['aws-settings']['s3-output-key']

        with open(model_data_path, mode='r') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter='|')

            # row_n = 0

            for idx, line in enumerate(csv_reader, start=1):
                row_n = idx

                lat_ = line['latitude']
                long_ = line['longitude']

                # Iteramos por todos los mapas y obtenemos la respuesta como un diccionario segun las columnas
                # de la configuracion
                # Actualizmos Line, diccionario original con la respuesta
                for item in self.geo_config:
                    response = self.do_georeverse_geopip(
                        lat=lat_,
                        long=long_,
                        columns=self.geo_config[item]['geojson_columns'],
                        geopip_=self.geo_config[item]['geopip']
                    )
                    line.update(
                        **response
                    )

                # Calculamos el geohash porque puede servir algo
                geohash_response = {'geohash': self.calcultate_geohash(lat=lat_, long=long_)}

                line.update(
                    **geohash_response
                )

                self.df_result.append(line)

                # if row_n % 50000 == 0:
                #     print(f"Vamor por: {row_n} registros")

                # Exportar y liberar memoria
            #     if row_n % 50000 == 0:
            #
            #         new_data_result_name = create_unique_file_name(
            #             event_name=f'nequi_georeverse_predictions_{str(row_n).zfill(3)}',
            #             extension="parquet", timestamp=self.ds
            #         )
            #         new_data_key_result_name: str = output_key + new_data_result_name
            #         self.model_config['aws-settings']['s3-output-key'] = new_data_key_result_name
            #
            #         self.make_file_predictions()
            #         self.upload_results()
            #         self.df_result = []
            #
            # # La colita, tambien se debe exportar
            # regs = str(len(self.df_result))
            # new_data_result_name = create_unique_file_name(
            #     event_name=f'nequi_georeverse_predictions_{str(regs).zfill(3)}',
            #     extension="parquet", timestamp=self.ds
            # )
            # new_data_key_result_name: str = output_key + new_data_result_name
            # self.model_config['aws-settings']['s3-output-key'] = new_data_key_result_name
            #
            # self.make_file_predictions()
            # self.df_result = []

    def make_file_predictions(self):
        """
        Export the result with predictions to a file (parquet format prefer)
        """
        # Data Obs: Logging ------------------------------------------------------------
        msg = f'make_file_predictions: Generando archivo con predicciones'
        DataObs.print_log(log_type='info', step=self.__class__.__name__, msg=msg)

        try:
            output_key = self.model_config['aws-settings']['s3-output-key']
            output_path = f'{output_key[output_key.rfind("/") + 1:]}'
            self.model_results_path = f'{Settings.APP_MODEL_RESULTS_PATH}{output_path}'
            df = pd.DataFrame(self.df_result)
            self.model_predictions += df['cod_dane'].count()
            self.model_predictions_failure += df.cod_dane[df.cod_dane == '-1'].count()
            # df.to_parquet(self.model_results_path, index=False, compression='snappy')
            df.to_json(self.model_results_path, orient='records', lines=True, force_ascii=False)
        except Exception as e:
            self.dataobs_event.update({
                'app_status_code': 415,
                'app_status_desc': 'Error generando archivo con predicciones'
            })
            msg = f"{self.dataobs_event['app_status_desc']} - Exception: {e}"
            DataObs.print_log(log_type='error', step=f"{self.__class__.__name__}", msg=msg)
            raise

    def run(self):
        try:
            self.load_config()  # Cargamos el yml con las rutas de AWS
            self.load_config_geomaps()  # Cargamos la configuracion de los geojson
            self.load_maps()  # Instanciamos los GeoPip

            # Creamos una lista con todos los archivos por los que vamos a iterar
            new_data_list: tuple = list_files_in_dir(Settings.APP_MODEL_DATA_PATH, '')
            new_data_list_size = len(new_data_list)

            data_key = self.model_config['aws-settings']['s3-data-key']
            output_key = self.model_config['aws-settings']['s3-output-key']

            # Iteramos por cada uno de los archivos en la carpeta de datos
            for filenum, file in enumerate(new_data_list, start=1):
                new_data_key_name = file[file.rfind("/") + 1:]

                # Crear nombre unico usando la fecha y un hash
                new_data_result_name = create_unique_file_name(
                    event_name=f'georeverse_results_{str(filenum).zfill(3)}',
                    extension='json', timestamp=self.ds
                )

                new_data_key_name: str = data_key + new_data_key_name
                new_data_key_result_name: str = output_key + new_data_result_name

                # Data Obs: Logging ------------------------------------------------------------
                msg = f'run [{filenum}][{new_data_list_size}]: Procesando {new_data_key_name}'
                DataObs.print_log(log_type='info', step=self.__class__.__name__, msg=msg)

                # Sobreescribimos las variables con el nombre del archivo que estamos iterando
                self.model_config['aws-settings']['s3-data-key'] = new_data_key_name
                self.model_config['aws-settings']['s3-output-key'] = new_data_key_result_name

                # Iteramos por cada uno de los registos.
                # Dentro del iterador, se generan los resultados y se montan a S3 de una vez

                self.row_iterator()

                if len(self.df_result) > 0:
                    self.make_file_predictions()

            # Actualizamos la cantidad de predicciones
            self.dataobs_event['app_payload'].update({
                'model_predictions': int(self.model_predictions),
                'model_predictions_failure': int(self.model_predictions_failure)
            })

        except Exception as e:
            msg = f"Error code: {self.dataobs_event['app_status_code']} \n" \
                  f"Error description: {self.dataobs_event['app_status_desc']} \n" \
                  f"Exception: {str(e)}"
            DataObs.print_log(log_type='error', step=self.__class__.__name__, msg=msg)
            raise e
        finally:
            # Data Obs: Tracking ------------------------------------------------------------
            final_time = time.time()
            self.dataobs_event.update({
                'app_runtime': final_time - self.current_time
            })

            # Data Obs: Logging ------------------------------------------------------------
            msg = f'Log generado en Data-Obs. Proceso finalizado.'
            DataObs.print_log(log_type='info', step=self.__class__.__name__, msg=msg)


if __name__ == '__main__':
    # Posibles mapas para cargar: 'geo_world', 'divipola_colombia', 'zona_urbana'
    # Si queremos solo cargar ciertos mapas, pasamos la variable geo_list = ['geo_world', 'divipola_colombia']
    # GeoReversePipeline(geo_list=['geo_world', 'divipola_colombia']).run()
    GeoReversePipeline().run()
