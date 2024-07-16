import requests
from bs4 import BeautifulSoup
import re
from confluent_kafka import Consumer, Producer
import json
import time

# Configuración de Kafka Consumer
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'scraper-url-consumer',
    'auto.offset.reset': 'earliest'
}

# Configuración de Kafka Producer
producer_config = {
    'bootstrap.servers': 'localhost:9093',
    'client.id': 'scraper-movies-producer'
}

def movie_details():
    consumer = Consumer(consumer_config)
    producer = Producer(producer_config)

    # Subscribirse al topic de URLs
    consumer.subscribe(['movie-urls'])
    #timeout = 30
    #start_time = time.time()
    try:
        c = 0
        empty_poll_count = 0
        # Número máximo de polls sin recibir mensajes antes de cerrar el bucle
        max_empty_polls = 2

        while True:
            # Esperar por un mensaje durante 1 segundo
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

            base_url = 'https://cuevanaseries.tv'
            url = msg.value().decode('utf-8')

            website_movies = f'{base_url}{url}'
            print(f"Processing URL: {website_movies}")

            try:
                response = requests.get(website_movies)
                if response.status_code != 200:
                    print(f"Error al acceder a {website_movies}, status code: {response.status_code}")
                    continue

                soup = BeautifulSoup(response.content, 'html.parser')
                movies = soup.find('article',class_='TPost')
                    
                if not movies:
                    print(f"Error al acceder a {website_movies}, status code: {response.status_code}")
                    continue

                image_url = movies.find('img').get('data-src')
                title = movies.find('h1', class_='Title').get_text(strip=True)
                
                rating_text = movies.find('span', class_='post-ratings').get_text(strip=True)
                
                # Extraer solo la parte decimal del rating
                match = re.search(r'\((\d+\.\d+)', rating_text)
                if match:
                    rating = float(match.group(1))
                else:
                    rating = 0

                meta_element = movies.find('p', class_='meta')
                duration = meta_element.find('span').get_text(strip=True)
            
                year = meta_element.find('a').get_text(strip=True)

                quality_element = movies.find('span', class_='Qlty')
                quality = quality_element.get_text(strip=True) if quality_element else 'Sin dato'
                description = movies.find('div', class_='Description').get_text(strip=True)
                
                views_match  = re.search(r'desde (\d+) usuarios', rating_text) 
                if views_match:
                    views = int(views_match.group(1))
                else:
                    views = 0

                # Obtener género, país, director y actores
                info_list = movies.find('ul', class_='InfoList')
                genre = []
                country = 'Sin dato'
                director = 'Sin dato'
                actors = []

                if info_list:
                    for li in info_list.find_all('li'):
                        strong_html = li.find('strong')
                        if strong_html:
                            strong_html_text = strong_html.get_text(strip=True)
                            if 'Género:' in strong_html_text:
                                genre = [a.get_text(strip=True) for a in li.find_all('a')]
                            elif 'País:' in strong_html_text:
                                country = li.find('a').get_text(strip=True)
                            elif 'Director:' in strong_html_text:
                                director = li.find('span', class_='color-w').get_text(strip=True)
                            elif 'Actores:' in strong_html_text:
                                actors = [a.get_text(strip=True) for a in li.find_all('a')]
                
                c += 1
                print(f'{c} - TITLE: {title}')
                if c == 41:
                    break
                movie_data = {
                    'image_url': image_url,
                    'title': title,
                    'rating': rating,
                    'duration': duration,
                    'year': year,
                    'quality': quality,
                    'description': description,
                    'genre': genre,
                    'country': country,
                    'director': director,
                    'actors': actors,
                    'views': views
                }

                # Enviar los detalles de la película a Kafka
                producer.produce('movie-details', key=title, value=json.dumps(movie_data))
                producer.flush()

    
            except Exception as e:
                print(f"Error al procesar {url}: {e}")
            '''
            current_time = time.time()
            print(current_time)
            if current_time - start_time > timeout:
                print("Tiempo de espera agotado")
                break
            '''
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush()