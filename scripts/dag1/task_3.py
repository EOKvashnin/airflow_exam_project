import pandas as pd
import os

file = pd.read_csv('/airflow/source/union_csv/combined.csv', encoding='Windows-1251')
file = file.sort_values(by=file.columns[0], ascending=True)
if os.path.exists('/airflow/source/union_csv/sort_combined.csv'):
    os.remove('/airflow/source/union_csv/sort_combined.csv')
    file.to_csv(os.path.join('/airflow/source/union_csv', 'sort_combined.csv'), sep=',', index=False, encoding='Windows-1251')
else:
    file.to_csv(os.path.join('/airflow/source/union_csv', 'sort_combined.csv'), sep=',', index=False,
                encoding='Windows-1251')