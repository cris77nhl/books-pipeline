"""
Bloque 3: Utilidades de Calidad y Normalización
Funciones para limpiar, normalizar y validar datos.
"""
import re
import pandas as pd

def normalize_date(date_str):
    if date_str is None or (isinstance(date_str, float) and pd.isna(date_str)) or (isinstance(date_str, pd._libs.missing.NAType)):
        return None
    s = str(date_str).strip()
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
        return s
    if re.match(r'^\d{4}-\d{2}$', s):
        return f"{s}-01"
    if re.match(r'^\d{4}$', s):
        return f"{s}-01-01"
    try:
        return pd.to_datetime(s, errors="coerce").strftime("%Y-%m-%d")
    except Exception:
        return None

def normalize_language(lang_code):
    if lang_code is None or (isinstance(lang_code, float) and pd.isna(lang_code)):
        return None
    return str(lang_code).strip().split('-')[0].lower()

def normalize_currency(currency_code):
    if currency_code is None or (isinstance(currency_code, float) and pd.isna(currency_code)):
        return None
    code = str(currency_code).strip().upper()
    return code if re.match(r'^[A-Z]{3}$', code) else None

def clean_string(text):
    """
    CORRECCIÓN: Limpia espacios y elimina comillas simples de inicio/fin.
    Esto es crucial para que los nombres de autores no queden con comillas extrañas.
    """
    if isinstance(text, str):
        cleaned = text.strip()
        # Elimina comillas simples si encierran todo el string (ej. 'John Doe')
        if cleaned.startswith("'") and cleaned.endswith("'"):
            return cleaned[1:-1].strip()
        return cleaned
    return text

def generate_quality_metrics(df_goodreads, df_google, df_dim_book):
    """
    Calcula las métricas de calidad solicitadas y las devuelve como dict.
    """
    def pct(n, d):
        return round((n / d * 100.0) if d else 0.0, 2)

    total_dim = len(df_dim_book)
    n_null_title = int(df_dim_book['title'].isnull().sum())
    n_null_isbn13 = int(df_dim_book['isbn13'].isnull().sum())
    n_null_price = int(df_dim_book['price_amount'].isnull().sum())
    n_null_pubdate = int(df_dim_book['pub_date_iso'].isnull().sum())

    metrics = {
        "fuentes": {
            "goodreads": {"filas_leidas": int(len(df_goodreads)), "columnas_leidas": int(len(df_goodreads.columns))},
            "googlebooks": {"filas_leidas": int(len(df_google)), "columnas_leidas": int(len(df_google.columns))}
        },
        "integracion": {
            "filas_canonicas_generadas": int(total_dim),
            "columnas_canonicas": int(len(df_dim_book.columns))
        },
        "completitud_final (dim_book)": {
            "nulos_titulo": n_null_title,
            "pct_nulos_titulo": pct(n_null_title, total_dim),
            "nulos_isbn13": n_null_isbn13,
            "pct_nulos_isbn13": pct(n_null_isbn13, total_dim),
            "nulos_precio": n_null_price,
            "pct_nulos_precio": pct(n_null_price, total_dim),
            "nulos_fecha_pub": n_null_pubdate,
            "pct_nulos_fecha_pub": pct(n_null_pubdate, total_dim),
        },
        "validaciones_formato (dim_book)": {
            "fechas_iso_validas": int(df_dim_book['pub_date_iso'].str.match(r'^\d{4}-\d{2}-\d{2}$', na=False).sum()),
            "monedas_iso_validas": int(df_dim_book['price_currency'].str.match(r'^[A-Z]{3}$', na=False).sum()),
            "idiomas_bcp47_validos": int(df_dim_book['language'].str.match(r'^[a-z]{2,3}$', na=False).sum())
        }
    }
    return metrics