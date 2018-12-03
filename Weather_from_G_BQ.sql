SELECT
  country,
  state,
  name,
  stn,
   	mo,
	ROUND(AVG((temp-32)*5/9),1) temp, 
	ROUND(AVG((min-32)*5/9),1) min, 
	ROUND(AVG((max-32)*5/9),1) max, 
	ROUND(AVG(IF(prcp=99.99,0,prcp)),1) avg_prcp, 
	ROUND(SUM(prcp),1) AS month_precip,     
	COUNT(prcp) AS rainy_days
	
	
FROM (
  SELECT
  country,
  state,
  name,
  stn,
    year,
	mo,
   temp,
    max,
	min,
  prcp,
      
    ROW_NUMBER() OVER(PARTITION BY state ORDER BY max DESC) rn
  FROM (
    SELECT
	  max,
	  min,
	  temp,
	  prcp,
	  year,
      mo,
       stn,
      wban
    FROM
      TABLE_QUERY([bigquery-public-data:noaa_gsod], 'table_id CONTAINS "gsod2010" OR table_id CONTAINS "gsod2011" OR table_id CONTAINS "gsod2012" OR table_id CONTAINS "gsod2013" OR table_id CONTAINS "gsod2014" OR table_id CONTAINS "gsod2015"')) a   -- 'REGEXP_MATCH(table_id,"gsod2010|gsod2011|gsod2012|gsod2013|gsod2014|gsod2015|gsod2016"')) a     --'table_id CONTAINS "gsod"')) a
  JOIN
    [bigquery-public-data:noaa_gsod.stations] b
  ON
    a.stn=b.usaf
    AND a.wban=b.wban
  WHERE
    state IS NOT NULL
	AND REGEXP_MATCH(year, '2010|2011|2012|2013|2014|2015|2016')
    AND max<1000
    )
WHERE
  rn=1
  GROUP BY
   country,
  state,
  name,
  stn,
  mo
  

 
  
  --WHERE Year