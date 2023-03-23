
with dim_delivery_method__source as (

  SELECT 
  *
  FROM `vit-lam-data.wide_world_importers.application__delivery_methods`
)

, dim_delivery_method__rename_column as (
  SELECT 
    delivery_method_id as delivery_method_key
    ,delivery_method_name as delivery_method_name 
  from dim_delivery_method__source
)

, dim_delivery_method__cast_type as (
  SELECT
    cast(delivery_method_key as integer) as delivery_method_key
    ,cast(delivery_method_name as string) as delivery_method_name
  from dim_delivery_method__rename_column
)

,dim_delivery_method_add_undefined_record as (
SELECT
  delivery_method_key
  ,delivery_method_name
from dim_delivery_method__cast_type

union all

SELECT 
  0 as delivery_method_key
  ,'Undefined' as delivery_method_name

union all

SELECT 
  -1 as delivery_method_key
  ,'Error' as delivery_method_name
)

SELECT
  delivery_method_key
  ,delivery_method_name
from 
  dim_delivery_method_add_undefined_record
