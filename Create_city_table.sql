--- СОЗДАНИЕ ТАБЛИЦЫ "CITY"      
      
CREATE TABLE City

DROP TABLE City

create table City (
	id serial PRIMARY KEY,
	city_in_api varchar(20),
	city_real varchar(20)
	)
	
INSERT INTO City(city_in_api, city_real)
SELECT DISTINCT city,
CASE 
	WHEN city = 'Волга' THEN 'Рыбинск'
	WHEN city = 'Sysert' THEN 'Екатеринбург'
	WHEN city = 'Заполярный' THEN 'Печенга'
	WHEN city = 'Nikolsk' THEN 'Никольск'
	WHEN city = 'Novaya Matsesta' THEN 'Сочи'
	ELSE city 
	END AS city_r
FROM api_wthr_to_db