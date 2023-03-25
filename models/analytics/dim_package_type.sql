
with dim_package_type__source as (

  SELECT 
  *
  FROM `vit-lam-data.wide_world_importers.warehouse__package_types`
)

, dim_package_type__rename_column as (
  SELECT 
    package_type_id as package_type_key
    ,package_type_name as package_type_name 
  from dim_package_type__source
)

, dim_package_type__cast_type as (
  SELECT
    cast(package_type_key as integer) as package_type_key
    ,cast(package_type_name as string) as package_type_name
  from dim_package_type__rename_column
)

,dim_package_type_add_undefined_record as (
SELECT
  package_type_key
  ,package_type_name
from dim_package_type__cast_type

union all

SELECT 
  0 as package_type_key
  ,'Undefined' as package_type_name

union all

SELECT 
  -1 as package_type_key
  ,'Error' as package_type_name
)

SELECT
  package_type_key
  ,package_type_name
from 
  dim_package_type_add_undefined_record
