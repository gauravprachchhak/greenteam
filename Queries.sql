SELECT COUNT(Name),Name,BgnDate FROM Alaska WHERE Name IS NOT NULL 

SELECT COUNT(Name), Name, BgnDate FROM Region WHERE LENGTH(Name) > 0 GROUP BY Name HAVING COUNT(Name) > 1 ORDER BY COUNT(Name) DESC

SELECT Name, BgnDate FROM Alaska WHERE LENGTH(Name) > 0 GROUP BY Name HAVING COUNT(Name) > 1 ORDER BY Name

SELECT glac_name, Area,src_date FROM glims WHERE glac_name='Colonia' GROUP BY src_date




select * from
(
select T.glac_name, count(*) as `num` FROM
(
SELECT glac_name, Area, src_date 
FROM GLIMS WHERE GLAC_NAME IN
( SELECT glac_name FROM glims WHERE LENGTH(glac_name) > 0 GROUP BY glac_name HAVING COUNT(glac_name) > 1 ORDER BY COUNT(glac_name) DESC
)
group by src_date, glac_name
order by glac_name
) T
group by (T.glac_name)
)
order by num desc

order by glac_name


SELECT glac_name FROM glims WHERE LENGTH(glac_name) > 0 GROUP BY glac_name HAVING COUNT(glac_name) > 1 ORDER BY COUNT(glac_name) DESC

SELECT count(glac_name),glac_name,src_date FROM glims WHERE LENGTH(glac_name) > 0 GROUP BY glac_name HAVING COUNT(glac_name) > 1 ORDER BY COUNT(glac_name) DESC








CREATE TABLE CARS_SOLD AS
SELECT Country AS Country,2005 AS Year,"2005" AS Cars FROM ALL_VEHICLES  
UNION SELECT Country,2006,"2006" FROM ALL_VEHICLES
UNION SELECT Country,2007,"2007" FROM ALL_VEHICLES
UNION SELECT Country,2008,"2008" FROM ALL_VEHICLES
UNION SELECT Country,2009,"2009" FROM ALL_VEHICLES
UNION SELECT Country,2010,"2010" FROM ALL_VEHICLES
UNION SELECT Country,2011,"2011" FROM ALL_VEHICLES
UNION SELECT Country,2012,"2012" FROM ALL_VEHICLES
UNION SELECT Country,2013,"2013" FROM ALL_VEHICLES
UNION SELECT Country,2014,"2014" FROM ALL_VEHICLES
UNION SELECT Country,2015,"2015" FROM ALL_VEHICLES
WHERE Country IS NOT NULL AND LENGTH(Country)>0

SELECT CARS_SOLD.Country,CARS_SOLD.Year FROM  CARS_SOLD 
JOIN GHCN_GHG 
ON (GHCN_GHG.Country = CARS_SOLD.Country)

CREATE TABLE GHCN_GHG_GCS AS
SELECT GHCN_GHG.*,CARS_SOLD.Cars FROM  CARS_SOLD 
JOIN GHCN_GHG 
ON (LOWER(GHCN_GHG.Country) = LOWER(CARS_SOLD.Country)) AND 
(LOWER(GHCN_GHG.Year) = LOWER(CARS_SOLD.Year))


SELECT * FROM GHCN_GHG
SELECT * FROM CARS_SOLD