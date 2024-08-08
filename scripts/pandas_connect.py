# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine

# Устанавливаем соединение с базой данных
engine = create_engine('postgresql://djeff07:Malina1905@194.87.147.124:5432/test_1')

# Запрос для выборки данных из таблицы
query = ('SELECT * '
         'FROM api_wthr_to_db '
         'WHERE city = \'Череповец\'')

# Чтение данных из базы данных в pandas DataFrame
df = pd.read_sql(query, engine)

# Вывод первых нескольких строк DataFrame
print(df.head(15))




