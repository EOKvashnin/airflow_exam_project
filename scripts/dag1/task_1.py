import requests
import yaml
import csv
import os
from datetime import datetime

url = "https://weatherapi-com.p.rapidapi.com/current.json"

headers = {"X-RapidAPI-Key": "ab46426aaamshab7cba2a1dc9d3bp1a34bbjsn236959a6cdb8",
           "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"}

# открываем Yaml-файл с координатами городов
with open(f'/airflow/scripts/dag1/city_coord.yaml') as file:
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
    writer.writerow(['Time', 'Country', 'Region', 'City', 'Temperature'])  # добавляем строку с заголовками

    for city, coord in cities_data.items():
        location = {'q': coord}  # для каждой координаты запрашиваем температуру
        response = requests.get(url, headers=headers, params=location)
        wthr = response.json()
        country = wthr.get('location').get('country')
        region = wthr.get('location').get('region')
        city_name = wthr.get('location').get('name')
        loc_time = loc_time_in_file
        #        loc_time = wthr.get('current').get('last_updated')
        temp_C = int(wthr.get('current').get('temp_c'))

        writer.writerow([loc_time, country, region, city_name, temp_C])  # Записываем данные в файл CSV

        print(
            f'Country: {country}, Region: {region}, City: {city_name}, Date_time: {loc_time}, Temp: {temp_C}')
        # Выводим на печать, что бы посмотреть, что записали в файл

    print(f'Данные успешно сохранены в файл {filename}')