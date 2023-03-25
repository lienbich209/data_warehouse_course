
with source_dim_supplier as (
  select * 
  from 
  `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, rename_source_dim_supplier as ( 
select 
  supplier_id as supplier_key
  ,supplier_name
  ,supplier_reference
  ,bank_account_name
  ,bank_account_branch
  ,bank_account_code
  ,bank_account_number
  ,bank_international_code
  ,payment_days
  ,supplier_category_id as supplier_category_key
  ,Primary_Contact_Person_id as Primary_Contact_Person_Key
  ,Alternate_Contact_Person_id as Alternate_Contact_Person_Key
  ,Delivery_Method_id as Delivery_Method_Key
  ,Delivery_City_id as Delivery_City_Key
from 
source_dim_supplier
)

,cast_source_dim_supplier as (
select 
  cast(supplier_key as integer)as supplier_key
  ,cast(supplier_name as string)as supplier_name
  ,cast(supplier_reference as string) as supplier_reference
  ,cast(bank_account_name as string) as bank_account_name
  ,cast(bank_account_branch as string) as bank_account_branch
  ,cast(bank_account_code as string) as bank_account_code
  ,cast(bank_account_number as integer) as bank_account_number
  ,cast(bank_international_code as integer) as bank_international_code
  ,cast(payment_days as integer) as payment_days
  ,cast(supplier_category_key as integer) as supplier_category_key
  ,cast(Primary_Contact_Person_key as integer) as Primary_Contact_Person_Key
  ,cast(Alternate_Contact_Person_key as integer) as Alternate_Contact_Person_Key
  ,cast(Delivery_Method_key as integer) as Delivery_Method_Key
  ,cast(Delivery_City_key as integer) as Delivery_City_Key
from 
  rename_source_dim_supplier 
)


,dim_supplier__convert_boolean as (
SELECT 
  *
FROM
  cast_source_dim_supplier
)

select 
   dim_supplier.supplier_key
  ,dim_supplier.supplier_name
  ,dim_supplier.supplier_reference
  ,dim_supplier.bank_account_name
  ,dim_supplier.bank_account_branch
  ,dim_supplier.bank_account_code
  ,dim_supplier.bank_account_number
  ,dim_supplier.bank_international_code
  ,dim_supplier.payment_days
  ,dim_supplier.supplier_category_key
  ,coalesce(dim_supplier_category.supplier_category_name,'Invalid') as supplier_category_name
  ,dim_supplier.Primary_Contact_Person_Key
  ,coalesce(dim_person_primary.full_name,'Invalid') as Primary_Contact_Person_Name
  ,dim_supplier.Alternate_Contact_Person_Key
  ,coalesce(dim_person_alternate.full_name,'Invalid') as Alternate_Contact_Person_Name
  ,dim_supplier.Delivery_Method_Key
  ,coalesce(dim_delivery.delivery_method_name,'Invalid') as delivery_method_name
  ,Delivery_City_Key
  ,coalesce(dim_City.city_name,'Invalid') as Delivery_City_Name
  ,coalesce(dim_City.state_province_key,-1) as state_province_key
  ,coalesce(dim_City.state_province_name,'Invalid') as state_province_name
  ,coalesce(dim_City.country_key,-1) as country_key
  ,coalesce(dim_City.country_name,'Invalid') as country_name
  
from dim_supplier__convert_boolean dim_supplier
left join {{ref('dim_supplier_category')}} as dim_supplier_category
on dim_supplier.supplier_category_key=dim_supplier_category.supplier_category_key

left join {{ref('dim_person')}} as dim_person_primary
on dim_supplier.Alternate_Contact_Person_Key = dim_person_primary.person_key

left join {{ref('dim_person')}} as dim_person_alternate
on dim_supplier.Primary_Contact_Person_Key = dim_person_alternate.person_key

left join {{ref('dim_delivery_method')}} as dim_delivery
on dim_supplier.delivery_method_key=dim_delivery.delivery_method_key

left join {{ref('dim_city')}} as dim_city
on dim_supplier.Delivery_City_Key=dim_City.city_key

