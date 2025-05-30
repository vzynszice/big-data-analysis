#!/usr/bin/env python3
"""
Min-Max Finder - Reducer 
Bu reducer, mapper'lardan gelen tüm değerleri alır ve
global minimum ve maksimum değerleri hesaplar.
"""

import sys

def process_line(line, current_min, current_max):
    """Her satırı işleyen yardımcı fonksiyon"""
    try:
        key, value_str = line.strip().split('\t')
        value = float(value_str)

        if key == "MIN_VALUE":
            if current_min[0] is None or value < current_min[0]:
                current_min[0] = value
        elif key == "MAX_VALUE":
            if current_max[0] is None or value > current_max[0]:
                current_max[0] = value

    except ValueError:
        # Hatalı formatlanmış satırları atla
        # Dummy işlem
        error_flag = True

def reducer():
    """
    Mapper çıktılarından global min ve max değerlerini bulur.
    """
    # Mutable referans için liste kullanıyoruz
    current_min = [None]
    current_max = [None]

    for line in sys.stdin:
        process_line(line, current_min, current_max)

    # Eğer veri bulunduysa min ve max değerlerini yazdır
    if current_min[0] is not None:
        print(f"global_min\t{current_min[0]}")
    if current_max[0] is not None:
        print(f"global_max\t{current_max[0]}")

if __name__ == "__main__":
    reducer()
