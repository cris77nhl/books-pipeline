"""
Bloque 3: Integración y Estandarización
(VERSION FINAL: Con correcciones de estabilidad de merge y columna 'author_primary' eliminada.)
"""

import sys
import json
import logging
import hashlib
from pathlib import Path
from datetime import datetime, UTC
import ast

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Permite ejecutar también como script directo: python src/integrate_pipeline.py
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR / "src"))

# Se asume que utils_isbn.py y utils_quality.py ya contienen las últimas correcciones
from utils_isbn import *
from utils_quality import *

# Rutas
LANDING_DIR = ROOT_DIR / "landing"
STANDARD_DIR = ROOT_DIR / "standard"
DOCS_DIR = ROOT_DIR / "docs"

# Entradas
GOODREADS_JSON_PATH = LANDING_DIR / "goodreads_books.json"
GOOGLEBOOKS_CSV_PATH = LANDING_DIR / "googlebooks_books.csv"

# Salidas
DIM_BOOK_PATH = STANDARD_DIR / "dim_book.parquet"
DETAIL_BOOK_PATH = STANDARD_DIR / "book_source_detail.parquet"
QUALITY_METRICS_PATH = DOCS_DIR / "quality_metrics.json"
SCHEMA_MD_PATH = DOCS_DIR / "schema.md"

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def create_directories():
    STANDARD_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)


def ensure_columns(df: pd.DataFrame, required_cols):
    df = df.copy()
    df.columns = df.columns.map(str).str.strip()
    missing = [c for c in required_cols if c not in df.columns]
    for c in missing:
        df[c] = pd.NA
    return df


def stable_hash(text: str) -> str:
    return hashlib.md5((text or "").encode("utf-8")).hexdigest()


def clean_isbn_series(s: pd.Series) -> pd.Series:
    s = (
        s.map(lambda x: None if pd.isna(x) else str(x))
        .str.replace("-", "", regex=False)
        .str.strip()
    )
    s = s.replace({"": pd.NA})
    return s


def norm_text(x):
    if x is None:
        return ""
    # Evita usar pd.isna sobre listas/tuplas/sets
    if isinstance(x, (list, tuple, set, dict)):
        return " ".join(
            str(clean_string(e))
            for e in x
            if e is not None and str(e).strip()
        ).strip().lower()
    try:
        if pd.isna(x):
            return ""
    except TypeError:
        # Tipos no escalar que pd.isna no sabe manejar
        pass
    return str(x).strip().lower()


def build_join_key(title, author_or_authors):
    """
    Construye la clave de unión a partir de título y autores.
    """
    if isinstance(author_or_authors, (list, tuple, set)):
        author_or_authors = " ".join(
            str(clean_string(a))
            for a in author_or_authors
            if a is not None and str(a).strip()
        )
    return f"{norm_text(title)}|{norm_text(author_or_authors)}"


def parse_list_string(s: pd.Series) -> pd.Series:
    """
    Convierte strings de lista (ej. "['a', 'b']") a listas reales de Python.
    """
    def attempt_parse(item):
        if pd.isna(item):
            return item
        try:
            return ast.literal_eval(str(item).strip())
        except (ValueError, SyntaxError):
            return [clean_string(str(item))]

    return s.apply(attempt_parse)


def load_goodreads():
    with open(GOODREADS_JSON_PATH, "r", encoding="utf-8") as f:
        payload = json.load(f)

    records = payload["books"] if isinstance(payload, dict) and "books" in payload else payload
    df = pd.DataFrame(records)

    required = ["title", "author", "rating", "ratings_count", "book_url", "isbn10", "isbn13"]
    df = ensure_columns(df, required)

    logging.info(f"Cargado {GOODREADS_JSON_PATH} ({len(df)} filas)")
    return df


def load_google():
    df = pd.read_csv(
        GOOGLEBOOKS_CSV_PATH,
        sep=",",
        encoding="utf-8",
        dtype={"isbn13": "string", "isbn10": "string"},
    )

    required = [
        "gb_id", "title", "subtitle", "authors", "publisher", "pub_date", "language",
        "categories", "isbn13", "isbn10", "price_amount", "price_currency",
        "goodreads_title_query", "goodreads_author_query",
    ]

    df = ensure_columns(df, required)
    logging.info(f"Cargado {GOOGLEBOOKS_CSV_PATH} ({len(df)} filas)")
    return df


def standardize_sources(df_gr: pd.DataFrame, df_gb: pd.DataFrame) -> pd.DataFrame:
    ts = datetime.now(UTC).isoformat()

    # Goodreads
    gr = df_gr.copy()
    gr = gr.rename(
        columns={
            "title": "title_gr", "author": "author_gr", "isbn10": "isbn10_gr", "isbn13": "isbn13_gr",
            "rating": "gr_rating", "ratings_count": "gr_ratings_count", "book_url": "gr_book_url",
        }
    )
    gr["source_gr"] = "goodreads"
    gr["source_file_gr"] = GOODREADS_JSON_PATH.name
    gr["ingestion_ts_gr"] = ts

    # Google Books
    gb = df_gb.copy()
    gb = gb.rename(
        columns={
            "title": "title_gb", "isbn10": "isbn10_gb", "isbn13": "isbn13_gb", "pub_date": "pub_date_raw",
            "language": "lang_raw", "price_currency": "currency_raw", "price_amount": "price_amount",
        }
    )
    gb["source_gb"] = "googlebooks"
    gb["source_file_gb"] = GOOGLEBOOKS_CSV_PATH.name
    gb["ingestion_ts_gb"] = ts

    # Parseo de listas en Google Books
    if "authors" in gb.columns:
        gb["authors"] = parse_list_string(gb["authors"])

    if "categories" in gb.columns:
        gb["categories"] = parse_list_string(gb["categories"])

    # Normaliza ISBN a texto sin guiones
    for col in ["isbn13_gb", "isbn10_gb"]:
        if col in gb.columns:
            gb[col] = clean_isbn_series(gb[col])

    for col in ["isbn13_gr", "isbn10_gr"]:
        if col in gr.columns:
            gr[col] = clean_isbn_series(gr[col])

    # join_key en ambos lados
    def build_join_key_for_gb(row):
        title = row.get("goodreads_title_query") or row.get("title_gb")
        author = row.get("goodreads_author_query") or row.get("authors")
        return build_join_key(title, author)

    gb["join_key"] = gb.apply(build_join_key_for_gb, axis=1)
    gr["join_key"] = gr.apply(
        lambda r: build_join_key(r.get("title_gr"), r.get("author_gr")),
        axis=1,
    )

    # Merge preferente por isbn13 (Outer Join)
    m_isbn = pd.merge(
        gb,
        gr,
        how="outer",
        left_on="isbn13_gb",
        right_on="isbn13_gr",
        suffixes=("_gb", "_gr"),
    )

    # Respaldo: merge por join_key (Outer Join)
    m_join = pd.merge(
        gb.add_prefix("jk_"),
        gr.add_prefix("jk_"),
        how="outer",
        left_on="jk_join_key",
        right_on="jk_join_key",
        suffixes=("_gb2", "_gr2"),
    )

    # Unificamos las claves de unión provenientes de cada lado tras el merge por ISBN
    m_isbn["join_key"] = m_isbn.get("join_key_gb").combine_first(m_isbn.get("join_key_gr"))

    # Fusión final combinando ambos merges
    m_join_cols = [
        "jk_join_key", "jk_title_gb", "jk_title_gr", "jk_author_gr", "jk_authors",
        "jk_isbn13_gb", "jk_isbn13_gr", "jk_isbn10_gb", "jk_isbn10_gr",
        "jk_publisher", "jk_pub_date_raw", "jk_lang_raw", "jk_currency_raw",
        "jk_price_amount", "jk_categories", "jk_gb_id", "jk_gr_book_url",
        "jk_gr_rating", "jk_gr_ratings_count",
    ]

    merged = pd.merge(
        m_isbn,
        m_join[m_join_cols],
        how="left",
        left_on="join_key",
        right_on="jk_join_key",
    )

    # --- UNIFICACIÓN CRÍTICA DE DATOS (RESOLUCIÓN DE VALUE ERROR) ---
    COALESCE_FINAL_COLS = [
        ("title_gb", "jk_title_gb"), ("title_gr", "jk_title_gr"),
        ("author_gr", "jk_author_gr"), ("authors", "jk_authors"),
        ("isbn13_gb", "jk_isbn13_gb"), ("isbn13_gr", "jk_isbn13_gr"),
        ("isbn10_gb", "jk_isbn10_gb"), ("isbn10_gr", "jk_isbn10_gr"),
        ("publisher", "jk_publisher"), ("pub_date_raw", "jk_pub_date_raw"),
        ("lang_raw", "jk_lang_raw"), ("currency_raw", "jk_currency_raw"),
        ("price_amount", "jk_price_amount"), ("categories", "jk_categories"),
        ("gb_id", "jk_gb_id"), ("gr_book_url", "jk_gr_book_url"),
        ("gr_rating", "jk_gr_rating"), ("gr_ratings_count", "jk_gr_ratings_count")
    ]

    # Utilizamos pd.Series.combine_first() para unificar de forma segura, eliminando el ValueError.
    for principal_col, backup_col in COALESCE_FINAL_COLS:
        if principal_col in merged.columns and backup_col in merged.columns:
            merged[principal_col] = merged[principal_col].combine_first(merged[backup_col])

    # Limpieza final de columnas de respaldo
    merged = merged.drop(
        columns=[col for col in merged.columns if col.startswith("jk_")],
        errors="ignore",
    )
    # -------------------------------------------------------------------------

    # book_id candidato
    def candidate_id(row):
        for key in ["isbn13_gb", "isbn13_gr", "isbn10_gb", "isbn10_gr"]:
            val = row.get(key)
            if pd.notna(val) and str(val).strip():
                return normalize_isbn(val)

        base = (
            f"{row.get('title_gb') or row.get('title_gr') or ''}|"
            f"{row.get('authors') or row.get('author_gr') or ''}|"
            f"{row.get('publisher') or ''}"
        )
        return stable_hash(base)

    merged["book_id"] = merged.apply(candidate_id, axis=1)

    logging.info(f"Fuentes unidas. Total filas pre-deduplicación: {len(merged)}")
    return merged


def apply_survival_rules(group: pd.DataFrame) -> pd.Series:
    """
    Lógica de supervivencia para elegir el Golden Record. (Se eliminó author_primary)
    """
    group = group.copy()
    group["__nonnulls"] = group.notna().sum(axis=1)
    group["__has_isbn"] = group[["isbn13_gb", "isbn13_gr", "isbn10_gb", "isbn10_gr"]].notna().any(axis=1)
    group = group.sort_values(["__has_isbn", "__nonnulls"], ascending=False)

    titles = (
        [t for t in group.get("title_gb", pd.Series(dtype=object)).dropna().tolist()]
        + [t for t in group.get("title_gr", pd.Series(dtype=object)).dropna().tolist()]
    )
    title = max(titles, key=len) if titles else None

    # Lógica para combinar correctamente los autores
    all_authors = []

    authors_gb_list = group.get("authors", pd.Series(dtype=object)).dropna().tolist()
    for item in authors_gb_list:
        if isinstance(item, list):
            all_authors.extend(item)
        elif pd.notna(item) and str(item):
            all_authors.append(clean_string(str(item)))

    author_gr_string = group.get("author_gr", pd.Series(dtype=object)).dropna().tolist()
    for item in author_gr_string:
        if pd.notna(item) and str(item):
            all_authors.append(clean_string(str(item)))

    unique_authors = sorted({clean_string(a) for a in all_authors if a})
    author_primary = unique_authors[0] if unique_authors else None # Aún se calcula para el 'title key'

    # Lógica para combinar correctamente las categorías
    all_cats = []
    cats_gb_list = group.get("categories", pd.Series(dtype=object)).dropna().tolist()
    for item in cats_gb_list:
        if isinstance(item, list):
            all_cats.extend(item)
        elif pd.notna(item) and str(item):
            all_cats.append(clean_string(str(item)))

    unique_categories = sorted({clean_string(c) for c in all_cats if c})

    s = group.iloc[0]

    return pd.Series(
        {
            "book_id": s["book_id"],
            "isbn13": s.get("isbn13_gb") if pd.notna(s.get("isbn13_gb")) else s.get("isbn13_gr"),
            "isbn10": s.get("isbn10_gb") if pd.notna(s.get("isbn10_gb")) else s.get("isbn10_gr"),
            "title": title,
            "subtitle": s.get("subtitle"),
            "authors": unique_authors,
            "categories": unique_categories,
            "publisher": s.get("publisher"),
            "pub_date_raw": s.get("pub_date_raw"),
            "language_raw": s.get("lang_raw"),
            "price_amount": s.get("price_amount"),
            "currency_raw": s.get("currency_raw"),
            "gr_rating": s.get("gr_rating"),
            "gr_ratings_count": s.get("gr_ratings_count"),
            "gr_book_url": s.get("gr_book_url"),
            "gb_id": s.get("gb_id"),
            "source_winner": "googlebooks" if pd.notna(s.get("gb_id")) else "goodreads",
            "ts_last_update": datetime.now(UTC).isoformat(),
        }
    )


def normalize_canonical_model(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["pub_date_iso"] = df["pub_date_raw"].apply(normalize_date)
    df["pub_year"] = pd.to_datetime(df["pub_date_iso"], errors="coerce").dt.year.astype("Int64")
    df["language"] = df["language_raw"].apply(normalize_language)
    df["price_currency"] = df["currency_raw"].apply(normalize_currency)

    df["title"] = df["title"].apply(clean_string)
    df["publisher"] = df["publisher"].apply(clean_string)

    df["categories"] = df["categories"].apply(
        lambda x: ", ".join(x) if isinstance(x, list) else x
    )

    # Columna author_primary ELIMINADA de dim_cols
    dim_cols = [
        "book_id", "isbn13", "isbn10", "title", "subtitle", "authors",
        "publisher", "pub_date_iso", "pub_year", "language", "categories",
        "price_amount", "price_currency", "gr_rating", "gr_ratings_count",
        "gb_id", "gr_book_url", "source_winner", "ts_last_update",
    ]

    df = ensure_columns(df, dim_cols)
    return df[dim_cols]


def generate_quality_metrics(df_gr: pd.DataFrame, df_gb: pd.DataFrame, df_dim: pd.DataFrame) -> dict:
    metrics = {
        "filas_por_fuente": {
            "goodreads": int(len(df_gr)), "googlebooks": int(len(df_gb)), "dim_book": int(len(df_dim)),
        },
        "pct_titulo_no_nulo": float(df_dim["title"].notna().mean()),
        "pct_isbn13_no_nulo": float(df_dim["isbn13"].notna().mean()),
        "pct_precio_no_nulo": float(df_dim["price_amount"].notna().mean()),
        "nulos_por_campo": {k: int(v) for k, v in df_dim.isna().sum().to_dict().items()},
        "duplicados_isbn13": int(df_dim["isbn13"].duplicated().sum()),
    }
    return metrics


def write_schema_md():
    schema_md = """# Esquema canónico dim_book

- book_id: isbn13 o hash estable cuando no hay isbn13. [str]
- title: título consolidado. [str]
- subtitle: subtítulo. [str]
- authors: lista unificada sin duplicados. [str]
- publisher: editorial. [str]
- pub_date_iso: ISO-8601 (YYYY-MM-DD). [str]
- pub_year: YYYY. [int]
- language: BCP-47 (es, en, pt-BR, ...). [str]
- isbn10: identificador ISBN-10. [str]
- isbn13: identificador ISBN-13. [str]
- categories: categorías separadas por coma. [str]
- price_amount: decimal con punto. [float]
- price_currency: ISO-4217 (USD, EUR, ...). [str]
- gr_rating, gr_ratings_count: rating y nº ratings de Goodreads. [float,int]
- gb_id: id de Google Books. [str]
- gr_book_url: URL Goodreads. [str]
- source_winner: googlebooks | goodreads. [str]
- ts_last_update: ISO-8601 UTC. [str]
"""
    SCHEMA_MD_PATH.write_text(schema_md, encoding="utf-8")


def integrate_pipeline():
    logging.info("--- Iniciando Bloque 3: Integración ---")
    create_directories()

    try:
        df_gr = load_goodreads()
        df_gb = load_google()
    except FileNotFoundError as e:
        logging.error(f"Faltan archivos de landing/: {e}")
        return
    except json.JSONDecodeError as e:
        logging.error(f"JSON inválido en {GOODREADS_JSON_PATH}: {e}")
        return

    # Bloque try/except para capturar fallos de procesamiento y logging crítico
    try:
        df_detail = standardize_sources(df_gr, df_gb)
        df_detail.to_parquet(DETAIL_BOOK_PATH, index=False, engine="pyarrow")
        logging.info(f"Guardado {DETAIL_BOOK_PATH} ({len(df_detail)} filas)")

        df_canonical = (
            df_detail.groupby("book_id", dropna=False)
            .apply(apply_survival_rules)
            .reset_index(drop=True)
        )
        logging.info(f"Filas después de deduplicación: {len(df_canonical)}")

        df_dim_book = normalize_canonical_model(df_canonical)
        df_dim_book.to_parquet(DIM_BOOK_PATH, index=False, engine="pyarrow")
        logging.info(f"Guardado {DIM_BOOK_PATH} ({len(df_dim_book)} filas)")

        quality = generate_quality_metrics(df_gr, df_gb, df_dim_book)
        with open(QUALITY_METRICS_PATH, "w", encoding="utf-8") as f:
            json.dump(quality, f, indent=2, ensure_ascii=False)

        write_schema_md()
        logging.info("--- Pipeline de Integración (Bloque 3) completado ---")
    except Exception as e:
        logging.critical(f"FALLO CRÍTICO EN PROCESAMIENTO: {type(e).__name__}: {e}")


if __name__ == "__main__":
    integrate_pipeline()