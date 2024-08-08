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
from config import url, headers
from base import Base
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--date", dest="date")
parser.add_argument("--host", dest="host")
parser.add_argument("--dbname", dest="dbname")
parser.add_argument("--user", dest="user")
parser.add_argument("--jdbc_password",dest="jdbc_password")
parser.add_argument("--port",dest="port")
args = parser.parse_args()

print('date = ' + str(args.date))
print('host = ' + str(args.host))
print('dbname = ' + str(args.dbname))
print('user = ' + str(args.user))
print('jdbc_password = ' + str(args.jdbc_password))
print('port = ' + str(args.port))

v_host = str(args.host)
v_dbname = str(args.dbname)
v_user = str(args.user)
v_password = str(args.jdbc_password)
v_port = str(args.port)

class Weather_from_dag(Base):
    __tablename__ = 'api_wthr_to_db'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    create_at = Column(TIMESTAMP, nullable=False, index=True)
    country = Column(VARCHAR(20), nullable=False)
    region = Column(VARCHAR(20), nullable=False)
    city = Column(VARCHAR(30), nullable=False)
    temperature = Column(Integer, nullable=False)

# Подключение к DB
SQLALCHEMY_DATABASE_URI = f'postgresql://{str(v_user)}:{str(v_password)}@{str(v_host)}:{str(v_port)}/{str(v_dbname)}'

# открываем Yaml-файл с координатами городов
with open('city_coord.yaml') as file:
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

file_path = f'source/{filename}'
if os.path.exists(file_path):
    os.remove(file_path)  # Удаляем файл, если он уже существует

with open(file_path, mode='a', newline='', encoding='cp1251') as file:
    writer = csv.writer(file, delimiter=';')
    writer.writerow(['create_at', 'country', 'region', 'city', 'temperature'])  # добавляем строку с заголовками

    for city, coord in cities_data.items():
        location = {'q': coord}  # для каждой координаты запрашиваем температуру
        response = requests.get(url, headers=headers, params=location)
        wthr = response.json()
        country = 'Россия'
        if 'location' in wthr:
            location_data = wthr.get('location')

            if location_data is not None and 'region' in location_data:
                region = location_data.get('region')
            else:
                region = None
        else:
            region = None
        if 'location' in wthr:
            location_data = wthr.get('location')

            if location_data is not None and 'name' in location_data:
                city_name = location_data.get('name')
            else:
                city_name = None
        else:
            city_name = None

        loc_time = loc_time_in_file

        if 'current' in wthr:
            location_data = wthr.get('current')

            if location_data is not None and 'name' in location_data:
                temp_C = location_data.get('temp_c')
            else:
                temp_C = "None"
        else:
            temp_C = "None"

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