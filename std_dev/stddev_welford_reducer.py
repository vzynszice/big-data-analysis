#!/usr/bin/env python3
"""
Standard Deviation - Welford's Online Algorithm Reducer 

Bu reducer, mapper'lardan gelen partial istatistikleri birleştirir
ve final ortalama ve standart sapmayı hesaplar.
"""

import sys
import math

def combine_statistics(n1, mean1, M2_1, n2, mean2, M2_2):
    """
    İki grup Welford istatistiğini birleştirir.
    
    Bu fonksiyon, distributed computing'in kalbinde yatar.
    Her mapper kendi veri parçası için istatistik hesaplar,
    bu fonksiyon onları birleştirir.
    """
    n = n1 + n2
    
    if n == 0:
        return 0, 0.0, 0.0
    
    # Ağırlıklı ortalama
    mean = (n1 * mean1 + n2 * mean2) / n
    
    # Delta: iki grubun ortalamalarının farkı
    delta = mean2 - mean1
    
    # M2'yi birleştir - bu formül Chan et al. (1979) tarafından türetilmiştir
    M2 = M2_1 + M2_2 + delta * delta * n1 * n2 / n
    
    return n, mean, M2

def process_stats_line(parts, total_stats, first_group):
    """
    STATS satırlarını işleyen yardımcı fonksiyon.
    
    Bu fonksiyon, mapper'lardan gelen istatistikleri okur ve
    birleştirir. total_stats ve first_group mutable referanslardır.
    """
    if parts[0] == "STATS" and len(parts) == 4:
        # Mapper istatistiklerini parse et
        n = int(parts[1])
        mean = float(parts[2])
        M2 = float(parts[3])
        
        if first_group[0]:
            # İlk grup - doğrudan ata
            total_stats[0] = n
            total_stats[1] = mean
            total_stats[2] = M2
            first_group[0] = False
        else:
            # Sonraki gruplar - birleştir
            combined = combine_statistics(
                total_stats[0], total_stats[1], total_stats[2],
                n, mean, M2
            )
            total_stats[0] = combined[0]  # n
            total_stats[1] = combined[1]  # mean
            total_stats[2] = combined[2]  # M2

def calculate_and_print_results(total_n, total_mean, total_M2):
    """
    Final istatistikleri hesaplar ve sonuçları yazdırır.
    
    Bu fonksiyon varyans, standart sapma hesaplar ve
    sonuçları formatlanmış şekilde çıktılar.
    """
    # Varyans = M2 / n
    variance = total_M2 / total_n
    
    # Standart sapma = √varyans
    std_dev = math.sqrt(variance)
    
    # Sample standard deviation (n-1 ile bölerek) - daha doğru tahmin
    if total_n > 1:
        sample_variance = total_M2 / (total_n - 1)
        sample_std_dev = math.sqrt(sample_variance)
    else:
        sample_variance = 0
        sample_std_dev = 0
    
    # Sonuçları yazdır
    print(f"=== PM2.5 İstatistikleri (Welford's Algorithm) ===")
    print(f"Toplam kayıt sayısı: {total_n}")
    print(f"Ortalama (μ): {total_mean:.4f} μg/m³")
    print(f"\n--- Population İstatistikleri (N ile bölme) ---")
    print(f"Population Varyans (σ²): {variance:.4f}")
    print(f"Population Standart Sapma (σ): {std_dev:.4f} μg/m³")
    print(f"\n--- Sample İstatistikleri (N-1 ile bölme) ---")
    print(f"Sample Varyans (s²): {sample_variance:.4f}")
    print(f"Sample Standart Sapma (s): {sample_std_dev:.4f} μg/m³")
    
    # Değişim katsayısı (Coefficient of Variation)
    cv = (std_dev / total_mean) * 100 if total_mean != 0 else 0
    print(f"\nDeğişim Katsayısı (CV): {cv:.2f}%")
    
    # Yorumlama
    print(f"\n=== Sonuçların Yorumu ===")
    print(f"Ortalama PM2.5 konsantrasyonu {total_mean:.2f} μg/m³")
    
    # WHO guideline'ları ile karşılaştırma
    if total_mean <= 5:
        print("✓ WHO yıllık hedef değeri (5 μg/m³) altında - Mükemmel!")
    elif total_mean <= 10:
        print("⚠ WHO ara hedef 4 (10 μg/m³) altında - İyi")
    elif total_mean <= 15:
        print("⚠ WHO ara hedef 3 (15 μg/m³) altında - Orta")
    else:
        print("⚠ WHO hedef değerlerinin üstünde - İyileştirme gerekli")
    
    # Değişkenlik yorumu
    print(f"\nDeğişkenlik analizi:")
    if cv < 20:
        print(f"CV=%{cv:.1f} - Düşük değişkenlik, tutarlı hava kalitesi")
    elif cv < 50:
        print(f"CV=%{cv:.1f} - Orta değişkenlik, makul dalgalanmalar")
    else:
        print(f"CV=%{cv:.1f} - Yüksek değişkenlik, önemli dalgalanmalar")

def reducer():
    """
    Mapper'lardan gelen partial istatistikleri birleştirir.
    
    Bu ana fonksiyon, tüm mapper çıktılarını okur,
    birleştirir ve final sonuçları hesaplar.
    """
    
    # Birleşik istatistikler - [n, mean, M2] olarak saklanır
    total_stats = [0, 0.0, 0.0]
    
    # İlk veri grubu flag'i
    first_group = [True]
    
    # Mapper çıktılarını oku
    for line in sys.stdin:
        try:
            parts = line.strip().split('\t')
            process_stats_line(parts, total_stats, first_group)
            
        except (ValueError, IndexError):
            # Hata durumunda dummy işlem
            error_occurred = True
    
    # Final istatistikleri hesapla ve yazdır
    if total_stats[0] > 0:
        calculate_and_print_results(total_stats[0], total_stats[1], total_stats[2])
    else:
        print("HATA: Hiç geçerli veri bulunamadı!")

if __name__ == "__main__":
    reducer()
