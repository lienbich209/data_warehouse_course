with fact_sales_order_line__source as (
SELECT *  
FROM `vit-lam-data.wide_world_importers.sales__order_lines` 
  )


, fact_sales_order_line__rename_column as (
select 
order_line_id as sales_order_line_key
,order_id as sales_order_key
,stock_item_id as product_key
,package_type_id as package_type_key
,quantity
,unit_price
,tax_rate
,description
,picked_quantity
from fact_sales_order_line__source
)


, fact_sales_order_line__cast_type as (
  SELECT
  cast(sales_order_line_key as integer) as sales_order_line_key
  ,cast(sales_order_key as integer)as sales_order_key
  ,cast(product_key as integer) as product_key
  ,cast(package_type_key as integer) as package_type_key
  ,cast(quantity as integer)as quantity
  ,cast(unit_price as numeric)as unit_price 
  ,cast(tax_rate as numeric) as tax_rate
  ,cast(description as string) as description
  ,cast(picked_quantity as integer) as picked_quantity
from fact_sales_order_line__rename_column
)


,fact_sales_order_line__calculate_measure as (
SELECT
*,quantity*unit_price as gross_amount
from fact_sales_order_line__cast_type
)


--,fact_sales_order_line as (

select 
fact_line.sales_order_line_key
,fact_line.sales_order_key
,fact_line.product_key
,coalesce(dim_product.product_name,'Invalid') as product_name
,coalesce(dim_product.brand_name,'Invalid') as brand_name
,coalesce(dim_product.is_chiller_stock,'Invalid') as is_chiller_stock
,coalesce(dim_product.supplier_key,-1) as supplier_key
,coalesce(dim_product.supplier_name,'Invalid') as supplier_name
,coalesce(dim_product.color_key,-1) as color_key
,coalesce(dim_product.color_name,'Invalid') as color_name
,fact_line.quantity
,fact_line.unit_price
,fact_line.gross_amount
,fact_line.tax_rate
,fact_line.description
,fact_line.picked_quantity
,fact_header.customer_key
,fact_header.order_date as order_date
,fact_header.is_undersupply_backordered
,coalesce (fact_header.picked_by_person_key,-1) as picked_by_person_key
,coalesce(fact_header.picked_by_person_name,'Invalid') as picked_by_person_name
,coalesce (fact_header.salesperson_person_key,-1) as salesperson_person_key
,coalesce(fact_header.salesperson_person_name,'Invalid') as salesperson_person_name
,coalesce (fact_header.contact_person_key,-1) as contact_person_key
,coalesce(fact_header.contact_person_name,'Invalid') as contact_person_name
,fact_header.expected_delivery_date as expected_delivery_date
,fact_person.full_name
,fact_line.package_type_key
,fact_package.package_type_name
from fact_sales_order_line__calculate_measure as fact_line
left join {{ref('stg_fact_sales_orders')}} as fact_header 
on fact_line.sales_order_key=fact_header.order_key
left join {{ref('dim_person')}} as fact_person
on fact_header.picked_by_person_key=fact_person.person_key
left join {{ref('dim_package_type')}} as fact_package
on fact_line.package_type_key=fact_package.package_type_key
left join {{ref('dim_product')}} as dim_product
on fact_line.product_key=dim_product.product_key

/*select 
  customer_key
  ,picked_by_person_key
  ,full_name
  ,sum(gross_amount) doanh_thu
  ,order_date
from fact_sales_order_line
group by customer_key, picked_by_person_key, full_name, order_date
order by doanh_thu*/

