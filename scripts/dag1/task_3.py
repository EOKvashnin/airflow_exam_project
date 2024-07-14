# -*- coding: windows-1251 -*-
import pandas as pd
import os

input_file_path = '/airflow/source/union_csv/combined.csv'
output_file_path = '/airflow/source/union_csv/sort_combined.csv'

if os.path.exists(input_file_path):
    file = pd.read_csv(input_file_path, encoding='Windows-1251')
    # сортируем файл по первому и 3-му столбцу
    file = file.sort_values(by=file.columns[0], ascending=False)

    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    file.to_csv(output_file_path, sep=',', index=False, encoding='Windows-1251')
    print(f'Данные успешно отсортированы и сохранены в файл {output_file_path}')
else:
    print(f'Файл {input_file_path} не найден')