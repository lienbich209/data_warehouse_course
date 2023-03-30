
with dim_supplier__source as (

  SELECT 
  *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_column as (
  SELECT 
    supplier_id as supplier_key
    ,supplier_name as supplier_name 
  from dim_supplier__source
)

, dim_supplier__cast_type as (
  SELECT
    cast(supplier_key as integer) as supplier_key
    ,cast(supplier_name as string) as supplier_name
  from dim_supplier__rename_column
)

SELECT
  supplier_key
  ,supplier_name
from dim_supplier__cast_type
