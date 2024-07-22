select id, row_number() over (partition by city order by create_at DESC) as n,create_at, city, temperature
from api_wthr_to_db

SELECT * FROM api_wthr_to_db
WHERE city = 'Череповец'
ORDER BY create_at DESC 

--Погода сегодня в Череповце
SELECT * FROM api_wthr_to_db
WHERE city = 'Череповец'
AND EXTRACT(DAY FROM create_at) = EXTRACT (DAY FROM now())



SELECT city, count(temperature)
FROM api_wthr_to_db
GROUP BY city 

-- Средняя, минимальная и максимальная температура в Череповце в июле с 7 утра до 7 вечера.
SELECT ROUND(avg(temperature), 2) AS avg_temp, min(temperature) AS min_temp, max(temperature) AS max_temp
FROM api_wthr_to_db
WHERE city = 'Череповец' 
AND date_part('month', create_at::date) = 7
AND EXTRACT(HOUR FROM create_at) > 7
AND EXTRACT(HOUR FROM create_at) < 19


SELECT api.create_at, api.Region, c.city_real, api.temperature
	FROM api_wthr_to_db AS api
	JOIN city AS c ON api.city = c.city_in_api
ORDER by api.create_at desc

SELECT count(id) AS cnt
FROM api_wthr_to_db

SELECT max(create_at) AS max_create
FROM api_wthr_to_db


SELECT c.city_real, ROUND(AVG (api.temperature), 0)
	FROM api_wthr_to_db api 
	JOIN city c
	ON api.city = c.city_in_api
where 
	date_part('month', create_at::date) = 7
	AND EXTRACT(HOUR FROM api.create_at) > 6
	AND EXTRACT(HOUR FROM api.create_at) < 19
GROUP by c.city_real
ORDER by c.city_real	
	