#!/usr/bin/env python3
import sys
import csv

# SABİT BAŞLIK LİSTESİ (orijinal dosyanızdaki sırayla)
EXPECTED_FIELDNAMES = ['date_local', 'state_name', 'county_name', 'arithmetic_mean', 'aqi', 'first_max_value', 'observation_count', 'latitude', 'longitude']
VALUE_COLUMN_NAME = 'arithmetic_mean'

def normalize(value, min_val, max_val):
    if max_val == min_val:
        return 0.5 
    return (value - min_val) / (max_val - min_val)

def process_valid_row(row, line_parts, writer, global_min_val, global_max_val, processed_count):
    """Geçerli bir satırı işleyen fonksiyon"""
    try:
        value_str = row.get(VALUE_COLUMN_NAME)
        
        if value_str is not None and value_str.strip() != "":
            try:
                original_value = float(value_str)
                normalized_value = normalize(original_value, global_min_val, global_max_val)
                row[VALUE_COLUMN_NAME] = f"{normalized_value:.8f}"
                processed_count[0] += 1
            except ValueError:
                # Float dönüşüm hatası - değeri değiştirmeden bırak
                error_occurred = True
        
        # Değiştirilmiş satırı EXPECTED_FIELDNAMES sırasına göre yaz
        output_line = [row.get(fn, '') for fn in EXPECTED_FIELDNAMES]
        writer.writerow(output_line)
        
    except Exception as e:
        # Genel hata durumunda orijinal satırı yaz
        writer.writerow(line_parts)

def mapper(global_min_val, global_max_val):
    # stdin'den satır satır oku
    reader = csv.reader(sys.stdin)
    
    # Başlıksız CSV çıktısı için writer
    writer = csv.writer(sys.stdout, lineterminator='\n')
    
    line_count = 0
    processed_count = [0]  # Mutable referans için liste
    
    for line_parts in reader:
        line_count += 1
        
        if len(line_parts) == len(EXPECTED_FIELDNAMES):
            # Satırı bir dictionary'e dönüştür
            row = dict(zip(EXPECTED_FIELDNAMES, line_parts))
            
            # Satırı işle
            process_valid_row(row, line_parts, writer, global_min_val, global_max_val, processed_count)
        else:
            # Farklı sayıda sütun - hiçbir şey yapma
            skipped_line = True

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(1)
    
    try:
        g_min = float(sys.argv[1])
        g_max = float(sys.argv[2])
    except ValueError:
        sys.exit(1)

    mapper(g_min, g_max)
