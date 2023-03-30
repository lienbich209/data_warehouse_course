
with dim_city__source as (

  SELECT 
  *
  FROM  `vit-lam-data.wide_world_importers.application__cities`
)

, dim_city__rename_column as (
  SELECT 
    city_id as city_key
    ,city_name as city_name 
    ,state_province_id as state_province_key
  from dim_city__source
) 

, dim_city__cast_type as (
  SELECT
    cast(city_key as integer) as city_key
    ,cast(city_name as string) as city_name
    ,cast(state_province_key as int) as state_province_key
  from dim_city__rename_column
)

,dim_city_add_undefined_record as (
SELECT
  city_key
  ,city_name
  ,state_province_key
from dim_city__cast_type

union all

SELECT 
  0 as city_key
  ,'Undefined' as city_name
  ,0 as state_province_key

union all

SELECT 
  -1 as city_key
  ,'Error' as city_name
  ,-1 as state_province_key
)

SELECT
  dim_city.city_key
  ,dim_city.city_name
  ,dim_city.state_province_key
  ,coalesce (dim_state_province.state_province_name,'Invalid') as state_province_name
  ,coalesce (dim_state_province.country_key, -1) as country_key
  ,coalesce (dim_state_province.country_name,'Invalid') as country_name
from 
  dim_city_add_undefined_record as dim_city
left join {{ref('dim_state_province')}} as dim_state_province
on dim_state_province.state_province_key = dim_state_province.state_province_key 
