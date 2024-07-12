import pandas as pd
import glob
import os

folder_path = '/airflow/source/'
files = glob.glob(folder_path + '*.csv')
print(folder_path)

combined = pd.DataFrame()

# Проверяем существование файла combined.csv и удаляем его, если он существует
if os.path.exists('/airflow/source/union_csv/combined.csv'):
    os.remove('/airflow/source/union_csv/combined.csv')
    print('Файл combined.csv удален')

for file in files:
    try:
        data = pd.read_csv(file, encoding='windows-1251')
        if not data.empty:
            header = data.iloc[:1]
            if combined.empty:
                combined = header
            combined = pd.concat([combined, data.iloc[0:]])
        else:
            print(f'Файл {file} пустой и будет пропущен')
    except pd.errors.EmptyDataError:
        print(f'Файл {file} не содержит данных')
    except Exception as e:
        print(f'Произошла ошибка при обработке файла {file}: {e}')
folder = '/airflow/source/union_csv/'
combined.to_csv(f'{folder}combined.csv', sep=',', index=False, encoding='windows-1251')

