with source_dim_customer as (
  select * 
  from 
  `vit-lam-data.wide_world_importers.sales__customers`
)

, rename_source_dim_customer as ( 
select 
  customer_id as customer_key
  ,customer_name
  ,customer_category_id as customer_category_key
  ,buying_group_id as buying_group_key 
from 
source_dim_customer
)

,cast_source_dim_customer as (
select 
  cast(customer_key as integer)as customer_key
  ,cast(customer_name as string)as customer_name
  ,cast(customer_category_key as integer) as customer_category_key
  ,cast(buying_group_key as integer) as buying_group_key
from 
  rename_source_dim_customer 
)

select 
  dim_customer.customer_key
  ,dim_customer.customer_name
  ,dim_customer.customer_category_key
  ,dim_customer_category.customer_category_name
  ,dim_customer.buying_group_key
  ,dim_buying_group.buying_group_name
from cast_source_dim_customer dim_customer
left join {{ref('stg_dim_customer_categories')}} as dim_customer_category
on dim_customer.customer_category_key=dim_customer_category.customer_category_key
left join {{ref('stg_dim_buying_groups')}} as dim_buying_group
on dim_customer.buying_group_key=dim_buying_group.buying_group_key
