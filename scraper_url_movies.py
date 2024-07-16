import requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer
import json

# Configuración de Kafka Producer
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'scraper-url-producer'
}

producer = Producer(producer_config)

def scrape_movies():
    base_url = f'https://cuevanaseries.tv'
    page_free_movies = f'{base_url}/nuevas-peliculas'
    page_number = 1
    urls_scraped = 0

    while True:
        pagination_movies = f'{page_free_movies}/page/{page_number}'
        response = requests.get(pagination_movies)
        print(pagination_movies)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            movies_items = soup.find_all('li', class_='xxx TPostMv')

            if not movies_items:
                return print("NO HAY PELICULAS")
                break
            
            for movie in movies_items:
                link_element = movie.find('a')

                if link_element:
                    link = link_element.get('href')
                    if link:
                        # Enviar la URL a Kafka
                        producer.produce('movie-urls', value=link)
                        producer.flush()
                else:
                    print(f"NO SE ENCONTRARON MÁS PELICULAS")
            
            next_page = soup.find('a', class_='next')
            if not next_page:
                break
            
            if page_number == 1:
                break
            
            page_number += 1
        else:
            break

    return urls_scraped > 0