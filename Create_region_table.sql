--Последняя дата обновления таблицы
SELECT max(create_at)::time
FROM api_wthr_to_db

select id, row_number() over (partition by city order by create_at DESC) as n,create_at, city, temperature
from api_wthr_to_db

SELECT * FROM api_wthr_to_db
WHERE city = 'Череповец'
ORDER BY create_at DESC 

--Погода сегодня в Череповце
SELECT * FROM api_wthr_to_db
WHERE city = 'Череповец'
AND EXTRACT(DAY FROM create_at) = EXTRACT (DAY FROM now())
ORDER BY create_at DESC 

--Количество позиций по каждому городу
SELECT c.city_real, count(temperature)
FROM api_wthr_to_db api
LEFT JOIN city c
ON api.city = c.city_real 
GROUP BY city_real 

-- Средняя, минимальная и максимальная температура в Череповце в июле с 7 утра до 7 вечера.
SELECT ROUND(avg(temperature), 2) AS avg_temp, min(temperature) AS min_temp, max(temperature) AS max_temp
FROM api_wthr_to_db
WHERE city = 'Череповец' 
AND date_part('month', create_at::date) = 7
AND EXTRACT(HOUR FROM create_at) > 7
AND EXTRACT(HOUR FROM create_at) < 19


SELECT api.create_at::date AS Дата, r.region_real AS Область, c.city_real AS Город, api.temperature AS Температура
	FROM api_wthr_to_db AS api
	JOIN city AS c ON api.city = c.city_in_api
	JOIN f_region AS r ON api.region = r.region_in_api
ORDER by api.create_at desc

SELECT count(id) AS cnt
FROM api_wthr_to_db

SELECT max(create_at) AS max_create
FROM api_wthr_to_db

-- Средняя температура по регионам в июле с 7 утра до 19 вечера
SELECT r.region_real, ROUND(AVG (api.temperature), 0)
	FROM api_wthr_to_db api 
	JOIN f_region r
	ON api.region = r.region_in_api
where 
	date_part('month', create_at::date) = 7
	AND EXTRACT(HOUR FROM api.create_at) > 6
	AND EXTRACT(HOUR FROM api.create_at) < 19
GROUP by r.region_real
ORDER by 2 DESC 

-- Регионы в таблице
SELECT DISTINCT region 
FROM api_wthr_to_db awtd 
	