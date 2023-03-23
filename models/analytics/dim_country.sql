
with dim_country__source as (

  SELECT 
  *
  FROM `vit-lam-data.wide_world_importers.application__countries`
)

, dim_country__rename_column as (
  SELECT 
    country_id as country_key
    ,country_name as country_name 
  from dim_country__source
)

, dim_country__cast_type as (
  SELECT
    cast(country_key as integer) as country_key
    ,cast(country_name as string) as country_name
  from dim_country__rename_column
)

,dim_country_add_undefined_record as (
SELECT
  country_key
  ,country_name
from dim_country__cast_type

union all

SELECT 
  0 as country_key
  ,'Undefined' as country_name

union all

SELECT 
  -1 as country_key
  ,'Error' as country_name
)

SELECT
  country_key
  ,country_name
from 
  dim_country_add_undefined_record
