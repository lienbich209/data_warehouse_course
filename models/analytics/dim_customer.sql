with source_dim_customer as (select * from `vit-lam-data.wide_world_importers.sales__customers`
)
, rename_source_dim_customer as ( select customer_id as customer_key, customer_name from source_dim_customer)

,cast_source_dim_customer as (select cast(customer_key as integer)as customer_key, cast(customer_name as string)as customer_name from rename_source_dim_customer )

select * from cast_source_dim_customer
