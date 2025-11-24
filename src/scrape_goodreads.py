"""
Bloque 1: Scraping de Goodreads (Con Anti-Popup y Paginación Robusta)
Genera landing/goodreads_books.json
"""
import json
import time
import logging
from datetime import datetime, UTC
from pathlib import Path

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Parsing
from bs4 import BeautifulSoup

# --- Configuración de Rutas ---
ROOT_DIR = Path(__file__).resolve().parents[1]
LANDING_DIR = ROOT_DIR / "landing"
GOODREADS_JSON_PATH = LANDING_DIR / "goodreads_books.json"

def create_directories():
    LANDING_DIR.mkdir(parents=True, exist_ok=True)

# --- Constantes ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
SEARCH_TERM = "one piece"
BASE_URL = "https://www.goodreads.com/search"
SEARCH_URL = f"{BASE_URL}?q={SEARCH_TERM}"
TARGET_COUNT = 30 
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Selectores
BOOK_CONTAINER = "tr[itemtype='http://schema.org/Book']"
TITLE_SEL = "a.bookTitle span[itemprop='name']"
AUTHOR_SEL = "a.authorName span[itemprop='name']"
RATING_SEL = "span.minirating"
URL_SEL = "a.bookTitle"
NEXT_PAGE_SEL = "a.next_page"
# Selectores para el botón "X" del popup de tu captura (se usarán en close_signin_popup)
POPUP_CLOSE_SELECTORS = [
    "div.Overlay__close", 
    "button[aria-label='Close']", 
    ".modal__close",
    "img[alt='Dismiss']"
]

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument(f"user-agent={USER_AGENT}")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def parse_rating(text):
    try:
        parts = text.split('—')
        avg = float(parts[0].replace('avg rating', '').strip())
        # Eliminamos comas para convertir el conteo
        count = int(parts[1].replace('ratings', '').replace('rating', '').replace(',', '').strip())
        return avg, count
    except:
        return None, None

def close_signin_popup(driver):
    """Intenta encontrar y cerrar el popup de registro si aparece."""
    for selector in POPUP_CLOSE_SELECTORS:
        try:
            # Buscamos el botón de cerrar con un timeout muy corto (1 seg)
            close_btn = driver.find_element(By.CSS_SELECTOR, selector)
            if close_btn.is_displayed():
                logging.info("Popup detectado. Intentando cerrar...")
                # Usamos execute_script para asegurar el clic si hay elementos superpuestos
                driver.execute_script("arguments[0].click();", close_btn)
                time.sleep(1) # Esperar a que desaparezca la animación
                return True
        except Exception:
            continue # Prurba el siguiente selector
    return False

def scrape_goodreads():
    logging.info(f"Iniciando scraping: '{SEARCH_TERM}' -> Meta: {TARGET_COUNT}")
    driver = setup_driver()
    books_data = []

    try:
        driver.get(SEARCH_URL)
        logging.info("Web cargada.")

        while len(books_data) < TARGET_COUNT:
            
            # 1. Esperar carga de libros
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, BOOK_CONTAINER)))
            except:
                logging.warning("No se cargaron libros.")
                break

            # 2. Extraer datos (BeautifulSoup)
            soup = BeautifulSoup(driver.page_source, 'lxml')
            items = soup.select(BOOK_CONTAINER)
            
            if not items: break

            logging.info(f"Pagina leída. Procesando {len(items)} libros...")

            for item in items:
                if len(books_data) >= TARGET_COUNT: break
                
                try:
                    t_el = item.select_one(TITLE_SEL)
                    title = t_el.text.strip() if t_el else "Unknown"
                    
                    # Evitar duplicados
                    if any(b['title'] == title for b in books_data): continue

                    a_el = item.select_one(AUTHOR_SEL)
                    u_el = item.select_one(URL_SEL)
                    r_el = item.select_one(RATING_SEL)
                    
                    r_txt = r_el.text.strip() if r_el else None
                    rate, count = parse_rating(r_txt) if r_txt else (None, None)
                    
                    # Se utiliza el split para asegurar la URL base
                    b_url = (BASE_URL.split('/search')[0] + u_el['href']) if u_el and u_el.has_attr('href') else None

                    books_data.append({
                        "title": title,
                        "author": a_el.text.strip() if a_el else None,
                        "rating": rate,
                        "ratings_count": count,
                        "book_url": b_url,
                        "isbn10": None, # Clave inicializada para el Enriquecimiento (Bloque 2)
                        "isbn13": None  # Clave inicializada para el Enriquecimiento (Bloque 2)
                    })
                    logging.info(f"[{len(books_data)}/{TARGET_COUNT}] + {title}")
                except Exception:
                    pass

            # 3. Paginación
            if len(books_data) >= TARGET_COUNT:
                logging.info("¡Meta alcanzada!")
                break

            # --- ZONA CRÍTICA: CAMBIO DE PÁGINA ---
            try:
                # A) Intentamos matar el popup
                close_signin_popup(driver)
                
                # B) Buscamos el botón Next
                next_btn = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, NEXT_PAGE_SEL))
                )
                
                # C) Scroll y clic
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                time.sleep(1)

                logging.info("Navegando a siguiente página...")
                driver.execute_script("arguments[0].click();", next_btn)
                
                # E) Espera para carga
                time.sleep(4)
                
            except Exception as e:
                logging.warning(f"No se pudo pasar de página (Fin o Bloqueo): {e}")
                break

    except Exception as e:
        logging.error(f"Error fatal: {e}")
    finally:
        driver.quit()

    # --- GUARDAR SOLO LA LISTA 'BOOKS' ---
    create_directories()
    if books_data:
        # Guardamos la lista de diccionarios directamente (sin la clave 'metadata')
        payload = books_data 
        
        with open(GOODREADS_JSON_PATH, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        logging.info(f"Listo. {len(books_data)} libros guardados en: {GOODREADS_JSON_PATH}")
    else:
        logging.warning("No hay datos.")

if __name__ == "__main__":
    scrape_goodreads()