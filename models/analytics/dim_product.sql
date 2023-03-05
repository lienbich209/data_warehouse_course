with 

dim_product_rename_column as(
SELECT 
  stock_item_id as product_key, stock_item_name as product_name, brand as brand_name
FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`),

source_dim_product
as (SELECT 
cast(product_key as integer) as product_key, cast(product_name as string) as product_name, cast(brand_name as string) as brand_name
FROM dim_product_rename_column)

select * from source_dim_product
