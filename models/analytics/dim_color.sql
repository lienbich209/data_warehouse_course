
with dim_color__source as (

  SELECT 
  *
  FROM `vit-lam-data.wide_world_importers.warehouse__colors`
)

, dim_color__rename_column as (
  SELECT 
    color_id as color_key
    ,color_name as color_name 
  from dim_color__source
)

, dim_color__cast_type as (
  SELECT
    cast(color_key as integer) as color_key
    ,cast(color_name as string) as color_name
  from dim_color__rename_column
)

,dim_color_add_undefined_record as (
SELECT
  color_key
  ,color_name
from dim_color__cast_type

union all

SELECT 
  0 as color_key
  ,'Undefined' as color_name

union all

SELECT 
  -1 as color_key
  ,'Error' as color_name
)

SELECT
  color_key
  ,color_name
from 
  dim_color_add_undefined_record
