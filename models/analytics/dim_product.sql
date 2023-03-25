with 

dim_product__source as(
SELECT 
      * 
FROM `vit-lam-data.wide_world_importers.warehouse__stock_items`
)

,dim_product__rename_column as (
  SELECT 
    stock_item_id as product_key
    ,stock_item_name as product_name
    ,brand as brand_name
    ,is_chiller_stock 
    ,supplier_id as supplier_key
    ,Barcode
    ,Tax_rate
    ,Unit_Price
    ,custom_fields
    ,tags
    ,color_id as color_key
    ,unit_package_id as unit_package_key
    ,outer_package_id as outer_package_key    
FROM
    dim_product__source
)

,dim_product__cast_type as (
  SELECT 
    cast(product_key as integer) as product_key
    ,cast(product_name as string) as product_name
    ,cast(brand_name as string) as brand_name
    ,cast(supplier_key as integer) as supplier_key
    ,cast(is_chiller_stock as boolean) as is_chiller_stock_boolean
    ,cast(Barcode as string) as Barcode
    ,cast(Tax_rate as numeric) as Tax_rate
    ,cast(Unit_Price as numeric) as Unit_Price
    ,cast(custom_fields as string) as custom_fields
    ,cast(tags as string) as tags
    ,cast(color_key as integer) as color_key
    ,cast(unit_package_key as integer) as unit_package_key
    ,cast(outer_package_key as integer) as outer_package_key    
FROM dim_product__rename_column
)

,dim_product__convert_boolean as (
Select 
  *
  ,case 
  when is_chiller_stock_boolean is true then 'Chiller Stock'
  when is_chiller_stock_boolean is false then 'Not Chiller Stock'
  when is_chiller_stock_boolean is null then 'Undefined'
  else 'Invalid' end as is_chiller_stock
from
  dim_product__cast_type
)
  
select 
  dim_product.product_key
  ,dim_product.product_name
  ,coalesce(dim_product.brand_name,'Undefined') as brand_name
  ,dim_product.is_chiller_stock
  ,dim_product.Barcode
  ,dim_product.Tax_rate
  ,dim_product.Unit_Price
  ,dim_product.custom_fields
  ,dim_product.tags
  ,dim_product.supplier_key
  ,coalesce (dim_supplier.supplier_name,'Undefined') as supplier_name
  ,coalesce (dim_supplier.supplier_category_key,0) as supplier_category_key
  ,coalesce (dim_supplier.supplier_category_name,'Undefined') as supplier_category_name
  ,dim_product.color_key
  ,coalesce (dim_color.color_name,'Undefined') as color_name
  ,dim_product.unit_package_key
  ,coalesce (dim_package_unit.package_type_name,'Undefined') as unit_type_name
  ,dim_product.outer_package_key    
  ,coalesce (dim_package_outer.package_type_name,'Undefined') as outer_type_name
from dim_product__convert_boolean as dim_product

left join {{ref('dim_supplier')}} as dim_supplier
on dim_product.supplier_key=dim_supplier.supplier_key

left join {{ref('dim_color')}} as dim_color
on dim_product.color_key=dim_color.color_key

left join {{ref('dim_package_type')}} as dim_package_unit
on dim_product.unit_package_key=dim_package_unit.package_type_key

left join {{ref('dim_package_type')}} as dim_package_outer
on dim_product.outer_package_key=dim_package_outer.package_type_key