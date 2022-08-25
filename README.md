# py-geo-reverse-col
Proyecto para hacer "reverse geocoding" a los insumos (CSV) que contengan como mínimo una longitud y una latitud válida.

- Se usa la libreria [GEOPIP: Geojson Point in Polygon](https://github.com/tammoippen/geopip)
- Los mapas se descargaron del [DANE-Geoportal](https://geoportal.dane.gov.co/servicios/descarga-y-metadatos/visor-descarga-geovisores/)
### Estructura del proyecto
```.
|-- catalog
|   `-- config.yml
|-- dataobs
|   `-- __init__.py
|-- geojson
|   |-- dane
|   |   |-- divipola_colombia
|   |   |   |-- data.geo.json
|   |   |   `-- metadatos.yml
|   |   `-- zona_urbana
|   |       |-- data.geo.json
|   |       `-- metadatos.yml
|   `-- mundo
|       `-- geo_world
|           |-- data.geo.json
|           `-- metadatos.yml
|-- model_data
|   |-- data
|   |   `-- loginservice_geoposition_0000_part_00
|   `-- results
|       `-- georeverse_results_001_2022-08-23-KJNYD48R.parquet
|-- settings
|   `-- __init__.py
|-- utils
|   `-- __init__.py
|-- README.md
|-- app.py
`-- requirements.txt
```

- **settings & dataobs & utils**: configuraciones generales, logger, variables estaticas, funciones genericas, etc.
- **catalog**: 
  - (obligatorio) config.yml: contiene las variables para una futura integracion con servicios de aws como S3. Se usa para generar los nombres de los archivos con los resultados.
- **geojson**: mapas en formato geojson, cada mapa debe contener por lo menos 2 archivos:
  - data.geo.json: geo json con los mapas que queremos hacer la busqueda
  - metadatos.yml: archivo con las propiedades que queremos extraer del geojson
- **model_data**: carpeta donde se almacenaran los datos de entrada y de salida
  - data: guarde acá sus datos de entrada
  - results: acá quedará el resultado

#### Nota:

Los geojson son bastante pesados, por ende no están cargados en el repo. Por fa descarguelos de forma local y los agrega a la carpeta que corresponda.

### Funcionamiento

Al ejecutar ```GeoReversePipeline().run()``` se itera por cada uno de los archivos que existan en la carpeta ```model_data/data```. 
Se hace una búsqueda, registro a registro y se generan archivos con las variables de entrada más las variables resultantes.

En el ```run()```, el flujo se ejecuta en el siguiente orden:
1. ```load_config()```: Se carga un diccionario con las opciones en el yml de la carpeta ```catalog```.
2. ```load_config_geomaps()```: Se carga un diccionario con los metadatos guardados en el yml en la carpeta ```geojson```.
3. ```load_maps()```: Se crean instancias de GeoPip con cada uno de los mapas guardados en la carpeta ```geojson```.
4. Se itera por cada uno de los archivos
5. Se itera por cada uno de los registros
6. Se hace la búsqueda usando ```geopip.search``` en la función ```do_georeverse_geopip()```
7. Se crea un archivo tipo json usando ```make_file_predictions()```.

### Tip
```
Mapas en este release: 'geo_world', 'divipola_colombia', 'zona_urbana'
Use: GeoReversePipeline(geo_list=['geo_world', 'divipola_colombia']).run()
con la lista de mapas que quiere cargar (geo_list).
```