"""
Bloque 2 y 3: Utilidades para manejar ISBNs.
"""
import re

def find_isbn(industry_identifiers, isbn_type='ISBN_13'):
    """
    Busca un ISBN específico (10 o 13) en la lista 'industryIdentifiers' de Google Books.
    """
    if not industry_identifiers:
        return None
    for identifier in industry_identifiers:
        if identifier.get('type') == isbn_type:
            return identifier.get('identifier')
    # Fallback: si no encuentra el tipo exacto
    if isbn_type == 'ISBN_13':
        for identifier in industry_identifiers:
            if identifier.get('type') == 'ISBN_10':
                return identifier.get('identifier')
    return None

def normalize_isbn(isbn):
    """
    Normaliza un ISBN a cadena sin guiones ni espacios.
    Preserva ceros a la izquierda y devuelve None si el valor es vacío.
    """
    if isbn is None:
        return None
    s = str(isbn).strip().replace("-", "").replace(" ", "")
    return s if s else None

def is_valid_isbn10(isbn):
    """
    Valida ISBN-10 (checksum). Devuelve True/False.
    """
    s = normalize_isbn(isbn)
    if not s or len(s) != 10 or not re.match(r'^\d{9}[\dXx]$', s):
        return False
    total = sum((10 - i) * (10 if ch in "Xx" else int(ch)) for i, ch in enumerate(s))
    return total % 11 == 0

def is_valid_isbn13(isbn):
    """
    Valida ISBN-13 (checksum). Devuelve True/False.
    """
    s = normalize_isbn(isbn)
    if not s or len(s) != 13 or not s.isdigit():
        return False
    total = sum((int(d) * (1 if i % 2 == 0 else 3)) for i, d in enumerate(s[:-1]))
    check = (10 - (total % 10)) % 10
    return check == int(s[-1])

def coalesce_isbn(isbn13, isbn10):
    """
    Devuelve isbn13 si existe; si no, isbn10; si no, None, normalizados.
    """
    s13 = normalize_isbn(isbn13)
    if s13:
        return s13
    s10 = normalize_isbn(isbn10)
    return s10 if s10 else None
