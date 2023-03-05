with fact_sales_order_source as
(SELECT * FROM `vit-lam-data.wide_world_importers.sales__orders` )

, fact_sales_order__rename_column as 
(select order_id as order_key, customer_id as customer_key from fact_sales_order_source)

, fact_sales_order__cast_type as
(select cast(order_key as integer) as order_key,cast(customer_key as integer) as customer_key from fact_sales_order__rename_column)

select order_key,customer_key from fact_sales_order__cast_type