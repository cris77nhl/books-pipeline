"""
Bloque 2: Enriquecimiento con Google Books API
Lee el JSON de goodreads, busca cada libro y guarda los resultados en un CSV.
"""

import requests
import json
import logging
import pandas as pd
from pathlib import Path

# Imports absolutos desde el paquete src
from utils_isbn import *
from utils_quality import *

# --- Definición de Rutas (Reemplaza a config.py) ---
ROOT_DIR = Path(__file__).resolve().parents[1]
LANDING_DIR = ROOT_DIR / "landing"
STANDARD_DIR = ROOT_DIR / "standard"
DOCS_DIR = ROOT_DIR / "docs"
GOODREADS_JSON_PATH = LANDING_DIR / "goodreads_books.json"
GOOGLEBOOKS_CSV_PATH = LANDING_DIR / "googlebooks_books.csv"


def create_directories():
    """Crea los directorios de salida si no existen."""
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    STANDARD_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
# --- Fin de Definición de Rutas ---


# --- Configuración ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# API pública sin API key
API_URL = "https://www.googleapis.com/books/v1/volumes"


def build_search_query(book):
    """Construye la query de búsqueda, priorizando ISBN si existe."""
    if book.get('isbn13'):
        return f"isbn:{book['isbn13']}"
    if book.get('isbn10'):
        return f"isbn:{book['isbn10']}"
    title = book.get('title', '')
    author = book.get('author', '')
    return f"intitle:{title}+inauthor:{author}"


def parse_google_book_data(item):
    """Parsea el complejo objeto JSON de la API de Google Books."""
    volume_info = item.get('volumeInfo', {})
    sale_info = item.get('saleInfo', {})
    identifiers = volume_info.get('industryIdentifiers', [])
    isbn13 = find_isbn(identifiers, 'ISBN_13')
    isbn10 = find_isbn(identifiers, 'ISBN_10')

    authors = volume_info.get('authors', []) or []
    categories = volume_info.get('categories', []) or []
    price_info = sale_info.get('listPrice', {}) or {}
    price_amount = price_info.get('amount')
    price_currency = price_info.get('currencyCode')

    return {
        "gb_id": item.get('id'),
        "title": volume_info.get('title'),
        "subtitle": volume_info.get('subtitle'),
        "authors": authors,
        "publisher": volume_info.get('publisher'),
        "pub_date": volume_info.get('publishedDate'),
        "language": volume_info.get('language'),
        "categories": categories,
        "isbn13": isbn13,
        "isbn10": isbn10,
        "price_amount": price_amount,
        "price_currency": price_currency,
    }


def enrich_books():
    """
    Función principal de enriquecimiento.
    Lee JSON, llama a la API y guarda en CSV.
    """
    create_directories()

    try:
        with open(GOODREADS_JSON_PATH, 'r', encoding='utf-8') as f:
            payload = json.load(f)
            # Admite {"metadata":..., "books":[...]} o lista directa
            goodreads_books = payload["books"] if isinstance(payload, dict) and "books" in payload else payload
    except FileNotFoundError:
        logging.error(f"Archivo no encontrado: {GOODREADS_JSON_PATH}. Ejecuta scrape_goodreads.py primero.")
        return
    except json.JSONDecodeError as e:
        logging.error(f"JSON inválido en {GOODREADS_JSON_PATH}: {e}")
        return

    logging.info(f"Cargados {len(goodreads_books)} libros desde {GOODREADS_JSON_PATH}")
    enriched_data = []

    for book in goodreads_books:
        query = build_search_query(book)
        params = {
            "q": query,
            "maxResults": 1
            # sin 'key': llamadas públicas sin API key
        }

        try:
            response = requests.get(API_URL, params=params, timeout=20)
            response.raise_for_status()
            data = response.json()

            if data.get('totalItems', 0) > 0 and 'items' in data:
                item = data['items'][0]
                parsed_data = parse_google_book_data(item)
                parsed_data['goodreads_title_query'] = book.get('title', '')
                parsed_data['goodreads_author_query'] = book.get('author', '')
                enriched_data.append(parsed_data)
                logging.info(f"Enriquecido: {parsed_data.get('title')} (buscado por: {book.get('title', '')})")
            else:
                logging.warning(f"No se encontraron resultados en Google Books para: {book.get('title', '')}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error en la API de Google Books para query '{query}': {e}")
        except Exception as e:
            logging.error(f"Error procesando libro {book.get('title', '')}: {e}")

    if enriched_data:
        df = pd.DataFrame(enriched_data)
        df.to_csv(
            GOOGLEBOOKS_CSV_PATH,
            index=False,
            sep=',',
            encoding='utf-8'
        )
        logging.info(f"Enriquecimiento finalizado. {len(df)} libros guardados en {GOOGLEBOOKS_CSV_PATH}")
    else:
        logging.warning("No se enriqueció ningún libro.")


if __name__ == "__main__":
    enrich_books()
