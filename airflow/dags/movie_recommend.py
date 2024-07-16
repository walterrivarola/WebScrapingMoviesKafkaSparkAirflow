import pprint
import requests
import json
import mysql.connector
import time

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from confluent_kafka import Consumer, Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType

def execute_post_url_fastapi(**kwargs):
    try:
        # Realiza la solicitud a la API de peliculas
        api_url = 'http://localhost:8000/scraper-url'
        response = requests.post(api_url)
        if response.status_code == 200:
            response_json = response.json()
            if response_json.get("message") == "Scraping de URLs completado":
                print("Scraping URL completed successfully")
                return response_json  # Return the response to be used in subsequent tasks if needed
            else:
                print("Scraping URL did not find any new URLs")
        else:
            print(f'Error response code: {response.status_code}')
    except KeyboardInterrupt:
        pass

'''
def execute_post_url_fastapi():
    try:
        # Realiza la solicitud a la API de peliculas
        api_url = 'http://localhost:8000/scraper-url'
        response = requests.post(api_url)
        if response.status_code == 200:
            print("Scraping URL running successfully")
    except Exception as e:
        print(f'Error al obtener las url de las peliculas: {str(e)}')
        return None
'''

def execute_post_details_fasapi():
    try:
        api_url = 'http://localhost:8000/scraper-movies-details'
        response = requests.post(api_url)
    except Exception as e:
        print(f'Error al obtener los detalles de las peliculas: {str(e)}')
        return None

def consume_kafka_movie_details(**kwargs):
    ti = kwargs['ti']
    consumer_config = {
           'bootstrap.servers': 'localhost:9093',
           'group.id': 'scraper-movies-producer',
           'auto.offset.reset': 'earliest'    
        }
    consumer = Consumer(consumer_config)
    consumer.subscribe(['movie-details'])
    empty_poll_count = 0
    try:
        max_empty_polls = 2
        all_details = []

        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                empty_poll_count += 1
                if empty_poll_count >= max_empty_polls:
                    print("No se recibieron más mensajes, terminando la ejecución.")
                    break
                continue
            else:
                empty_poll_count = 0  # Resetear el contador si se recibe un mensaje

            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            details = msg.value().decode('utf-8')
            #all_details.append(details)
            #json_all_details = json.dumps(all_details)
            ti.xcom_push(key='json_data_movie', value=details)
        #ti.xcom_push(key='json_data_movie', value=json_all_details)
        #pprint.pprint(json_all_details)
        #return json_all_details
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    
'''
def json_serialization(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='execute_post_details_fasapi')
    
    if json_data:
        json_message = json.dumps(json_data)
        producer.produce('movie-recommendation', value=json_message)
        producer.flush()
    else:
        print("No se pudo obtener el JSON de datos de la pelicula")
'''

def transform_with_spark(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='consume_kafka_movie_details')
    if json_data:
        try:
            spark = SparkSession.builder.appName('MovieDataTransformation').getOrCreate()
            sc = spark.sparkContext

            # Decodificar JSON
            json_data = json.loads(json_data)
            pprint.pprint(f"json_data: {json_data}")
            # Convertir JSON a RDD y luego a DataFrame
            rdd = sc.parallelize([json_data])
            df = spark.read.json(rdd)

            pprint.pprint(f"printSchema: {df.printSchema()}")
            pprint.pprint(f"SHOW: {df.show()}")
            # Función para convertir la duración de horas y minutos a minutos
            def convert_duration(duration):
                hours, minutes = duration.split('h ')
                hours = int(hours)
                minutes = int(minutes[:-1])
                total_minutes = hours * 60 + minutes
                
                return total_minutes
            
            # Registrar la función UDF para Spark SQL
            convert_duration_udf = udf(convert_duration, IntegerType())

            # Aplicar la transformación de duración
            df = df.withColumn('duration', convert_duration_udf(col('duration')))

            # Función para eliminar la parte inicial de la descripción
            def clean_description(description):
                return description.split('Cuevana3.', 1)[1].strip() if 'Cuevana3.' in description else description
            
            # Registrar la función UDF para Spark SQL
            clean_description_udf = udf(clean_description, StringType())

            # Aplicar la transformación de descripción
            df = df.withColumn('description', clean_description_udf(col('description')))
            
            # Convertir DataFrame a JSON y pasarlo a la siguiente tarea
            #transformed_data = df.toJSON().collect()
            transformed_data = [row.asDict() for row in df.collect()]
            #transformed_json = json.loads(transformed_data[0])
            ti.xcom_push(key='transformed_data', value=transformed_data)
            
            return transformed_data

        except Exception as e:
            print(f"Error al transformar datos con Spark: {str(e)}")
    else:
        print("No se pudo obtener el JSON de datos de la pelicula")

def insert_into_mysql(**kwargs):
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='transform_with_spark')
    if json_data:
        # Asegurarse de que json_data es una lista de diccionarios
        if isinstance(json_data, str):
            json_data = json.loads(json_data)

        try:
            # Conectar a la base de datos MySQL
            conn = mysql.connector.connect(
                host='localhost',
                user='root',
                password='root',
                database='cuevana3'
            )

            cursor = conn.cursor()

            # Extraer datos del JSON
            for movie in json_data:
                title = movie['title']
                years = movie['year']
                quality = movie['quality']
                duration = movie['duration']
                rating = movie['rating']
                descriptions = movie['description']
                genre = ', '.join(movie['genre'])
                director = movie['director']
                actors = ', '.join(movie['actors'])
                img_url = movie['image_url']
                country = movie['country']
                views = movie['views']

                # Insertar datos en la tabla MySQL
                insert_query = """
                INSERT INTO movies (title, years, quality, duration, rating, descriptions, genre, director,
                actors, img_url, country, views) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """

                cursor.execute(insert_query,(title,years,quality,duration,rating,descriptions,genre,
                                            director,actors,img_url,country,views))
                conn.commit()

            # Cerrar la conexión
            cursor.close()
            conn.close()

        except mysql.connector.Error as err:
            print(f"Error al insertar en MySQL: {err}")
    else:
        print("No se pudo obtener el JSON de datos de la pelicula")

dag = DAG('api_cuevana3_movie', 
          schedule_interval=timedelta(seconds=15),
          start_date=datetime(2024, 7, 1))


task1 = PythonOperator(
    task_id='execute_post_url_fastapi',
    python_callable=execute_post_url_fastapi,
    dag=dag,
    provide_context=True,
)

task2 = PythonOperator(
    task_id='execute_post_details_fasapi',
    python_callable=execute_post_details_fasapi,
    dag=dag,
    provide_context=True
)

task3 = PythonOperator(
    task_id='consume_kafka_movie_details',
    python_callable=consume_kafka_movie_details,
    dag=dag,
    provide_context=True
)

task4 = PythonOperator(
    task_id='transform_with_spark',
    python_callable=transform_with_spark,
    dag=dag,
    provide_context=True
)

task5 = PythonOperator(
    task_id='insert_into_mysql',
    python_callable=insert_into_mysql,
    dag=dag,
    provide_context=True
)

# Definir las dependencias
task1 >> task2 >> [task3, task4, task5]
#task1 >> task2 >> task4 >> task5 >> task3