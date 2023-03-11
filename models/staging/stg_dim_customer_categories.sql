with stg_dim_customer_categories__source as
(SELECT * FROM `vit-lam-data.wide_world_importers.sales__customer_categories` )

, stg_dim_customer_categories__rename_column as 
(select customer_category_id as customer_category_key, customer_category_name  from stg_dim_customer_categories__source)

, stg_dim_customer_categories__cast_type as (
select 
  cast (customer_category_key as integer) as customer_category_key
  ,cast(customer_category_name as string) as customer_category_name 
from 
  stg_dim_customer_categories__rename_column
  )

select customer_category_key,customer_category_name from stg_dim_customer_categories__cast_type