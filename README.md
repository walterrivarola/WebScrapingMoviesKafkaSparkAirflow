# Proyecto de Web Scraping - Peliculas

## Descripción

Este proyecto realiza scraping del sitio web [Cuevanaseries.tv](https://cuevanaseries.tv) para obtener información detallada de películas. Inicialmente, se obtienen las URLs de cada película del catálogo y, posteriormente, se realiza un scraping más profundo para extraer datos como título, actores, año de lanzamiento, duración, ratings, directores, descripción, entre otros.

## Tecnologías Utilizadas

- **Python 3.10**
- **FastAPI**: Para crear el servidor y manejar las solicitudes HTTP.
- **Airflow**: Para orquestar el flujo de datos.
- **Kafka**: Para la retención y gestión de mensajes.
- **Spark**: Para realizar transformaciones en los datos.
- **MySQL**: Para almacenar los datos extraídos.
- **Librerías de Python**:
  - `pprint`
  - `requests`
  - `json`
  - `mysql.connector`
  - `datetime`
  - `airflow`
  - `confluent_kafka`
  - `pyspark`
  - `beautifulsoup4`
  - `re`
  - `fastapi`

## Instalación

1. Clonar el repositorio:
    ```bash
    git clone https://github.com/walterrivarola/WebScrapingMoviesKafkaSparkAirflow.git
    
    cd WebScrapingMoviesKafkaSparkAirflow
    ```

2. Crear un entorno virtual y activarlo:
    ```bash
    python -m venv venv
    source venv/bin/activate # En Windows: venv\Scripts\activate
    ```

3. Instalar las dependencias:
    ```bash
    pip install -r requirements.txt
    ```

## Configuración

1. Configurar y levantar los servicios de Kafka y Spark utilizando Docker Compose:
    ```bash
    docker-compose up -d
    ```

## Uso

1. Iniciar el servidor FastAPI:
    ```bash
    uvicorn main:app --reload
    ```

2. Configurar y ejecutar los DAGs de Airflow para orquestar las tareas:
    ```bash
    # Comandos para iniciar Airflow
    export AIRFLOW_HOME=(LA RUTA EN DONDE ESTA EL PROYECTO)
    airflow standalone # para activar el servidor Airflow
    ```

3. Los endpoints disponibles en FastAPI son:
    - `POST /scraper-url`: Obtiene las URLs de las películas del catálogo.
    - `POST /scraper-movies-details`: Obtiene los detalles de cada película.
    - `GET /info-movies`: Obtiene información de las películas desde la base de datos.

## Estructura del Proyecto
├── Airflow/<br>
│ ├── dags/<br>
│ └── movie_recommend.py # Gestión de Airflow<br>
├── Spark/ # Configuración de Spark<br>
├── Kafka/ # Configuración de Kafka<br>
├── docker-compose.yml # Configuración de Kafka y Spark<br>
├── Dockerfile<br>
├── main.py # Archivo principal de FastAPI<br>
├── requirements.txt # Dependencias del proyecto<br>
├── scraper_url_movies.py # Script para obtener URLs de películas<br>
└── scraper_movies_details.py # Script para obtener datos de películas
