# books-pipeline

Pipeline completo para la extracción, enriquecimiento e integración de datos de libros desde Goodreads y Google Books. Incluye scraping, API, consolidación canónica y control de calidad.

---

## Estructura del repositorio

books-pipeline/
├─ README.md
├─ requirements.txt
├─ .env.example
├─ landing/
│ ├─ goodreads_books.json
│ └─ googlebooks_books.csv
├─ standard/
│ ├─ dim_book.parquet
│ └─ book_source_detail.parquet
├─ docs/
│ ├─ schema.md
│ └─ quality_metrics.json
└─ src/
├─ scrape_goodreads.py
├─ enrich_googlebooks.py
├─ integrate_pipeline.py
├─ utils_quality.py
└─ utils_isbn.py


---

## Descripción de bloques

### 1. Scraping Goodreads → JSON

- Extrae 10-15 libros desde una búsqueda pública (“data science”) en Goodreads.
- Campos: title, author, rating, ratings_count, book_url, isbn10, isbn13.
- Guarda en `landing/goodreads_books.json` con estructura estándar:
{
"metadata": {
"source": "goodreads_search",
"query": "data science",
"search_url": "...",
"user_agent": "...",
"fetch_datetime": "...",
"n_records": 15
},
"books": [ ... ]
}

text
- Documenta en el README: URL, selectores, user-agent, fecha, nº de registros.

### 2. Enriquecimiento Google Books → CSV

- Busca cada libro de Goodreads en la API pública de Google Books (por ISBN o título+autor).
- Campos: gb_id, title, subtitle, authors, publisher, pub_date, language, categories, isbn13, isbn10, price_amount, price_currency.
- Guarda en `landing/googlebooks_books.csv` (sep=",", UTF-8).
- Explícitamente NO se requiere API key para búsquedas públicas simples (limitadas por cuota Google).

### 3. Integración y estandarización → Parquet

- Lee archivos en `landing/`.
- Anota metadatos, controla calidad y normaliza: fechas ISO, idioma BCP-47 (`es`, `en`), moneda ISO-4217 (`EUR`, `USD`).
- Deduplicación por isbn13 y reglas de supervivencia: preferir registros más completos, unión de autores y categorías.
- Genera:
- `standard/dim_book.parquet` (libros únicos, modelo canónico)
- `standard/book_source_detail.parquet` (detalle por fuente)
- `docs/quality_metrics.json` (completitud, nulos, métricas)
- `docs/schema.md` (descripción de campos y reglas)

---

## Ejecución del pipeline

Instala dependencias:

pip install -r requirements.txt


Ejecuta paso a paso (desde raíz del repo):

python src/scrape_goodreads.py
python src/enrich_googlebooks.py
python src/integrate_pipeline.py

O si tienes problemas con los imports:
python -m src.scrape_goodreads
python -m src.enrich_googlebooks
python -m src.integrate_pipeline


---

## Decisiones clave y notas técnicas

- El scraping utiliza Selenium + BeautifulSoup y finge un usuario real mediante user-agent y mitigación de fingerprint.
- El JSON de Goodreads incluye metadata para trazabilidad.
- Google Books API se llama sin clave (`key`) para uso educativo/personal.
- Unión de fuentes robusta: primero por ISBN13, luego por título+autor normalizado.
- Todos los identificadores se tratan como strings para evitar conflictos de tipo.
- Integración emite parquet, métricas de calidad y un schema bien documentado.
- Los archivos en `landing/` no deben modificarse durante la integración.

---

## Metadatos técnicos

- User-agent usado: Mozilla/5.0 (Windows NT 10.0; Win64; x64)
- Selectores principales: `tr[itemtype='http://schema.org/Book']`, `.bookTitle`, `.authorName`, `.minirating`
- Separador CSV: `,`
- Codificación: `UTF-8`
- Campos normalizados: fechas (ISO-8601), idioma (BCP-47), moneda (ISO-4217)
- Decisiones pipeline: modelo canónico por ISBN13, deduplicación y unión de autores/categorías

---

## Licencia, uso y buenas prácticas

Proyecto educativo. Respeta los términos de uso de Goodreads y Google Books. Los archivos de scraping y API se guardan para entorno académico y no se debe automatizar a gran escala sin autorización.
