with dim_date__generate as (
  SELECT
    *
  FROM UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2030-12-31', interval 1 day)) AS date
)

,dim_date__enrich as (
select 
  *
  ,format_date('%A', date) as day_of_week
  ,format_date('%a', date) as day_of_week_short
  ,date_trunc(date,month) as year_month
  ,format_date('%B', date) as month
  ,date_trunc(date,year) as year
  ,EXTRACT(YEAR FROM date) AS year_number
from 
  dim_date__generate
)

select 
  *,
  case when day_of_week_short in ('Mon','Tue','Wed','Thu','Fri') then 'Weekday'
      when day_of_week_short in ('Sat','Sun') then 'Weekend'
      ELSE 'Invalid'
      end 
  as is_weekday_or_weekend 
from
  dim_date__enrich