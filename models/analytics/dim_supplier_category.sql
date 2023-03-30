
with dim_supplier_category__source as (

  SELECT 
  *
  FROM `vit-lam-data.wide_world_importers.purchasing__supplier_categories`
)

, dim_supplier_category__rename_column as (
  SELECT 
    supplier_category_id as supplier_category_key
    ,supplier_category_name as supplier_category_name 
  from dim_supplier_category__source
)

, dim_supplier_category__cast_type as (
  SELECT
    cast(supplier_category_key as integer) as supplier_category_key
    ,cast(supplier_category_name as string) as supplier_category_name
  from dim_supplier_category__rename_column
)

,dim_supplier_category_add_undefined_record as (
SELECT
  supplier_category_key
  ,supplier_category_name
from dim_supplier_category__cast_type

union all

SELECT 
  0 as supplier_category_key
  ,'Undefined' as supplier_category_name

union all

SELECT 
  -1 as supplier_category_key
  ,'Error' as supplier_category_name
)

SELECT
  supplier_category_key
  ,supplier_category_name
from 
  dim_supplier_category_add_undefined_record
