#!/usr/bin/env python3
"""
Min-Max Finder - Mapper 

Bu mapper, 'arithmetic_mean' sütunundaki her değeri okur ve
hem minimum hem de maksimum hesaplaması için reducer'a gönderir.
"""

import sys
import csv

VALUE_COLUMN_NAME = 'arithmetic_mean'

def process_row(row):
    """Satırı işleyen yardımcı fonksiyon"""
    try:
        value_str = row.get(VALUE_COLUMN_NAME)
        if value_str is not None and value_str.strip() != "":
            value = float(value_str)
            print(f"MIN_VALUE\t{value}")
            print(f"MAX_VALUE\t{value}")
    except (ValueError, TypeError, KeyError) as e:
        # Hatalı satırları veya eksik değerleri atla
        # Dummy işlem - hiçbir şey yapmıyoruz
        error_occurred = True  # Sadece bir değişken ataması

def mapper():
    """
    Giriş CSV dosyasındaki belirtilen sütundaki değerleri okur.
    Her değer için (MIN_VALUE, value) ve (MAX_VALUE, value) yayar.
    """
    csv_reader = csv.DictReader(sys.stdin)

    for row in csv_reader:
        process_row(row)

if __name__ == "__main__":
    mapper()
