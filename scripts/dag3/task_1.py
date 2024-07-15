# -*- coding: utf-8 -*-
import requests
import yaml
import os
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, VARCHAR, create_engine, TIMESTAMP
from sqlalchemy.orm import sessionmaker
import csv
from datetime import datetime
from config import url, headers, SQLALCHEMY_DATABASE_URI
from base import Base

class Weather_from_dag(Base):
    __tablename__ = 'api_wthr_to_db'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    create_at = Column(TIMESTAMP, nullable=False, index=True)
    country = Column(VARCHAR(20), nullable=False)
    region = Column(VARCHAR(20), nullable=False)
    city = Column(VARCHAR(30), nullable=False)
    temperature = Column(Integer, nullable=False)


# открываем Yaml-файл с координатами городов
with open(f'/airflow/scripts/dag3/city_coord.yaml') as file:
    cities_data = yaml.safe_load(file)  # записываем в cities_data содержимое файла через безопасный загрузчик

location = {'q': '59.1311, 37.8822'}  # запрашиваем температуру в Череповце для получения актуального времени
response = requests.get(url, headers=headers, params=location)
wthr = response.json()
loc_time_f = wthr.get('current').get('last_updated')  # Получаем дату выгрузки от API
# Меняем формат для представления даты в формате ГГГГ-ММ-ДД_ЧЧ-ММ
loc_time_formatted = datetime.strptime(loc_time_f, '%Y-%m-%d %H:%M').strftime('%Y-%m-%d_%H-%M')
# Меняем формат для представления даты в формате ГГГГ-ММ-ДДЧЧ-ММ для загрузки в файл
loc_time_in_file = datetime.strptime(loc_time_f, '%Y-%m-%d %H:%M').strftime('%Y.%m.%d %H:%M')
filename = f'weather_data_{loc_time_formatted}.csv'  # Формируем название файлов

file_path = f'/airflow/source/{filename}'
if os.path.exists(file_path):
    os.remove(file_path)  # Удаляем файл, если он уже существует

with open(file_path, mode='a', newline='', encoding='cp1251') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(['create_at', 'country', 'region', 'city', 'temperature'])  # добавляем строку с заголовками

    for city, coord in cities_data.items():
        location = {'q': coord}  # для каждой координаты запрашиваем температуру
        response = requests.get(url, headers=headers, params=location)
        wthr = response.json()
        country = wthr.get('location').get('country')
        region = wthr.get('location').get('region')
        city_name = wthr.get('location').get('name')
        loc_time = loc_time_in_file
        temp_C = int(wthr.get('current').get('temp_c'))

        writer.writerow([loc_time, country, region, city_name, temp_C])  # Записываем данные в файл CSV

        print(
            f'Country: {country}, Region: {region}, City: {city_name}, Date_time: {loc_time}, Temp: {temp_C}')
        # Выводим на печать, что бы посмотреть, что записали в файл

    print(f'Данные успешно сохранены в файл {filename}')

engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=True)
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session_local = SessionLocal()

# Открытие CSV файла и чтение его содержимого
with open(f'/airflow/source/{filename}', 'r', encoding='cp1251') as file:
    reader = csv.DictReader(file, delimiter=';')

    for row in reader:
        new_record = Weather_from_dag(
            create_at=datetime.strptime(row['create_at'], "%Y.%m.%d %H:%M").strftime("%Y-%m-%d %H:%M:%S"),
            country=row['country'],
            region=row['region'],
            city=row['city'],
            temperature=int(row['temperature'])
        )
        session_local.add(new_record)

# Фиксация изменений в базе данных
session_local.commit()