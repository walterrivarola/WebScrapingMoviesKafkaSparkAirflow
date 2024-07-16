from fastapi import FastAPI
from scraper_url_movies import scrape_movies
from scaper_movies_details import movie_details
from confluent_kafka import Producer
import json

app = FastAPI()

# Routes
@app.get("/")
def read_root():
    return {"message": "Hola mundo"}
'''
@app.post("/scraper-url")
def scrape_web_site():
    try:
        scrape_movies()
        return {"message": "Scraping de URLs iniciado"}
    except Exception as e:
        return {"message": "Error durante URL scraping", "error": str(e)}
'''

@app.post("/scraper-url")
def scrape_web_site():
    try:
        scraping_done = scrape_movies()
        return {"message": "Scraping de URLs completado" if scraping_done else "No se encontraron más URLs para scrapear"}
    except Exception as e:
        return {"message": "Error durante URL scraping", "error": str(e)}

@app.post("/scraper-movies-details")
def scrape_details():
    try:
        movie_details()  # movie_details ahora tomará los datos de Kafka
        return {"message": "Details scraping iniciado"}
    except Exception as e:
        return {"message": "Error durante details scraping", "error": str(e)}
    
@app.get("/info-movies")
def info_movies():
    return {"message": "Data is being sent to Kafka"}