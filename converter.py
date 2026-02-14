import pandas as pd
import sys
import os

def csv_to_xlsx(csv_file, xlsx_file=None):
    """
    Конвертирует CSV файл в XLSX
    """
    if xlsx_file is None:
        xlsx_file = os.path.splitext(csv_file)[0] + '.xlsx'
    
    try:
        # Читаем CSV файл
        df = pd.read_csv(csv_file)
        df = df.drop_duplicates()
        # Записываем в XLSX
        df.to_excel(xlsx_file, index=False)
        
        print(f"Файл {csv_file} успешно конвертирован в {xlsx_file}")
        
    except Exception as e:
        print(f"Ошибка при конвертации: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python script.py input.csv [output.xlsx]")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    xlsx_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    csv_to_xlsx(csv_file, xlsx_file)