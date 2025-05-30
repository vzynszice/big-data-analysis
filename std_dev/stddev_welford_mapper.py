#!/usr/bin/env python3
"""
Standard Deviation - Welford's Online Algorithm Mapper 

Bu mapper, Welford's algoritmasını kullanarak tek geçişte
ortalama ve varyans için gerekli istatistikleri hesaplar.
"""

import sys
import csv

def welford_update(value, n, mean, M2):
    """
    Welford algoritması güncelleme adımı.
    
    Bu fonksiyon, yeni bir değer geldiğinde istatistikleri günceller.
    Parametreler mutable referans (liste) olarak geçirilir.
    """
    # Welford's update formülü
    n[0] += 1
    delta = value - mean[0]
    mean[0] += delta / n[0]
    delta2 = value - mean[0]
    M2[0] += delta * delta2

def process_row(row, n, mean, M2):
    """
    CSV satırını işleyen yardımcı fonksiyon.
    
    Bu fonksiyon, her satır için değeri okur ve
    geçerliyse Welford güncellemesini yapar.
    """
    try:
        # PM2.5 değerini oku
        value = float(row['arithmetic_mean'])
        
        # Geçerli değerleri işle (negatif olmayan)
        if value >= 0:
            welford_update(value, n, mean, M2)
        else:
            # Negatif değer - dummy işlem
            negative_value_found = True
            
    except (ValueError, KeyError):
        # Hata durumunda dummy işlem
        error_in_row = True

def mapper():
    """
    Her veri parçası için local istatistikleri hesaplar.
    
    Welford's algoritması şu değişkenleri tutar:
    - n: Görülen değer sayısı
    - mean: Şu ana kadarki ortalama
    - M2: Sapmaların karelerinin toplamı
    """
    
    # Local istatistikler - mutable referanslar için liste kullanıyoruz
    n = [0]
    mean = [0.0]
    M2 = [0.0]
    
    # CSV reader oluştur
    csv_reader = csv.DictReader(sys.stdin)
    
    # Her satırı işle
    for row in csv_reader:
        process_row(row, n, mean, M2)
    
    # Mapper'ın local istatistiklerini emit et
    if n[0] > 0:
        # Reducer'ın birleştirmesi için özel format
        # FORMAT: STATS n mean M2
        print(f"STATS\t{n[0]}\t{mean[0]}\t{M2[0]}")
        
        # Debug bilgisi 
        print(f"DEBUG: Mapper {n[0]} değer işledi, local mean={mean[0]:.4f}", 
              file=sys.stderr)

if __name__ == "__main__":
    mapper()
