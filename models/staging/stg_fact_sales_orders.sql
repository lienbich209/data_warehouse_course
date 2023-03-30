with fact_sales_order_source as
(SELECT * FROM `vit-lam-data.wide_world_importers.sales__orders` )

, fact_sales_order__rename_column as (
select 
  order_id as order_key
  ,backorder_order_id as backorder_order_key
  ,is_undersupply_backordered
  ,order_date
  ,expected_delivery_date
  ,customer_id as customer_key
  ,picked_by_person_id as picked_by_person_key
  ,salesperson_person_id as salesperson_person_key
  ,contact_person_id as contact_person_key
from 
  fact_sales_order_source)

, fact_sales_order__cast_type as(
select 
  cast(order_key as integer) as order_key
  ,cast(backorder_order_key as integer) as backorder_order_key
  ,cast(is_undersupply_backordered as boolean) as is_undersupply_backordered_boolean
  ,cast(order_date as date) as order_date
  ,cast(expected_delivery_date as date) as expected_delivery_date
  ,cast(customer_key as integer) as customer_key
  ,cast(picked_by_person_key as integer) as picked_by_person_key
  ,cast(salesperson_person_key as integer) salesperson_person_key
  ,cast(contact_person_key as integer) as contact_person_key
  
from 
fact_sales_order__rename_column)


,fact_sales_order__convert_boolean as (
SELECT 
  *
  ,case
    when is_undersupply_backordered_boolean is true then 'Undersupple Backorder'
    when is_undersupply_backordered_boolean is false then 'Not Undersupple Backorder'
    when is_undersupply_backordered_boolean is null then 'Undefined'
    else 'Invalid' end as is_undersupply_backordered
FROM
  fact_sales_order__cast_type
)


select 
  fact_header.order_key
  ,fact_header.backorder_order_key
  ,fact_header.is_undersupply_backordered
  ,fact_header.customer_key 
  ,dim_customer.customer_name
  ,coalesce (fact_header.picked_by_person_key,0) as picked_by_person_key
  ,coalesce(dim_person_picked.full_name,'Invalid') as picked_by_person_name
  ,coalesce(fact_header.salesperson_person_key,0)as salesperson_person_key
  ,coalesce(dim_person_salesperson.full_name,'Invalid') as salesperson_person_name
  ,coalesce(fact_header.contact_person_key,0)as contact_person_key
  ,coalesce(dim_person_contact.full_name,'Invalid') as contact_person_name
  ,fact_header.order_date
  ,fact_header.expected_delivery_date
from 
  fact_sales_order__convert_boolean as fact_header
left join {{ref('dim_customer')}} as dim_customer
on fact_header.customer_key = dim_customer.customer_key 
left join {{ref('dim_person')}} as dim_person_picked
on fact_header.picked_by_person_key = dim_person_picked.person_key 
left join {{ref('dim_person')}} as dim_person_salesperson
on fact_header.salesperson_person_key = dim_person_salesperson.person_key
left join {{ref('dim_person')}} as dim_person_contact
on fact_header.contact_person_key = dim_person_contact.person_key  

