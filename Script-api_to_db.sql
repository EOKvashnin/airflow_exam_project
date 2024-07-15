select * from api_wthr_to_db


delete from api_wthr_to_db
where id=6

insert into api_wthr_to_db
select '2024-07-15 20:45:00.000' as Create_at,
		'Russia' as Country,
		'Yaric' as Region,
		'Ribinsk' as City,
		'333' as Temperature 
		
INSERT INTO api_wthr_to_db (create_at, country, region, city, temperature)
SELECT '2024-07-15 20:45:00.000' as create_at,
       'Russia' as country,
       'Yaric' as region,
       'Ribinsk' as city,
       '333' as temperature;
      
drop table api_wthr_to_db