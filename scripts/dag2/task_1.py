# -*- coding: utf-8 -*-
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, VARCHAR, create_engine, TIMESTAMP
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Sequence
import csv
from datetime import datetime


Base = declarative_base()
SQLALCHEMY_DATABASE_URI = f'postgresql://djeff07:Malina1905@194.87.147.124:5432/test_1'
class Weather_from_dag(Base):
    __tablename__ = 'Weather_from_dag'
    id = Column(Integer, nullable=False, unique=True, primary_key=True, autoincrement=True)
    Time = Column(TIMESTAMP, nullable=False, index=True)
    Country = Column(VARCHAR(20), nullable=False)
    Region = Column(VARCHAR(20), nullable=False)
    City = Column(VARCHAR(30), nullable=False)
    Temperature = Column(Integer, nullable=False)

engine = create_engine(SQLALCHEMY_DATABASE_URI, echo=True)

Base.metadata.create_all(bind=engine)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session_local = SessionLocal()

# Проверяем, есть ли записи в таблице
if session_local.query(Weather_from_dag).count() > 0:
    # Удаляем все существующие записи
    session_local.query(Weather_from_dag).delete()

# Открытие CSV файла и чтение его содержимого
#with open('/airflow/source/union_csv/sort_combined.csv', 'r') as file:
with open('sort_combined.csv', 'r') as file:
    reader = csv.DictReader(file, delimiter=';')
    print(reader)
    for row in reader:
        new_record = Weather_from_dag(
            Time=datetime.strptime(row['Time'], "%d.%m.%Y %H:%M").strftime("%Y-%m-%d %H:%M:%S"),
            Country = row['Country'],
            Region=row['Region'],
            City=row['City'],
            Temperature=int(row['Temperature'])
        )
        session_local.add(new_record)

# Фиксация изменений в базе данных
session_local.commit()