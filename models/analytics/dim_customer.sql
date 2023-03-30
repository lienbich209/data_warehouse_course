with source_dim_customer as (
  select * 
  from 
  `vit-lam-data.wide_world_importers.sales__customers`
)

, rename_source_dim_customer as ( 
select 
  customer_id as customer_key
  ,customer_name
  ,bill_to_customer_id as bill_to_customer_key
  ,credit_limit
  ,account_opened_date
  ,standard_discount_percentage
  ,payment_days
  ,phone_number
  ,fax_number
  ,delivery_run
  ,run_position
  ,delivery_postal_code
  ,postal_postal_code
  ,is_statement_sent as is_statement_sent_boolean
  ,is_on_credit_hold as is_on_credit_hold_boolean
  ,primary_contact_person_id as primary_contact_person_key
  ,alternate_contact_person_id as alternate_contact_person_key
  ,delivery_method_id as delivery_method_key
  ,delivery_city_id as delivery_city_key
  ,postal_city_id as postal_city_key 
  ,buying_group_id as buying_group_key
  ,customer_category_id as customer_category_key
FROM
source_dim_customer
)

,cast_source_dim_customer as (
SELECT
  cast(customer_key as integer)as customer_key
  ,cast(customer_name as string)as customer_name
  ,cast(bill_to_customer_key as integer) as bill_to_customer_key
  ,cast(credit_limit as integer) as credit_limit
  ,cast(account_opened_date as date) as account_opened_date 
  ,cast(standard_discount_percentage as numeric) as standard_discount_percentage
  ,cast(is_statement_sent_boolean as boolean) as is_statement_sent_boolean
  ,cast(payment_days as integer) as payment_days
  ,cast(phone_number as string) as phone_number
  ,cast(fax_number as string) as fax_number
  ,cast(delivery_run as string) as delivery_run
  ,cast(run_position as string) as run_position
  ,cast(delivery_postal_code as string) as delivery_postal_code
  ,cast(postal_postal_code as string) as postal_postal_code
  ,cast(is_on_credit_hold_boolean as boolean) as is_on_credit_hold_boolean
  ,cast(primary_contact_person_key as integer) as primary_contact_person_key
  ,cast(alternate_contact_person_key as integer) as alternate_contact_person_key
  ,cast(delivery_method_key as integer) as delivery_method_key
  ,cast(delivery_city_key as integer) as delivery_city_key
  ,cast(postal_city_key as integer) as postal_city_key
  ,cast(buying_group_key as integer) as buying_group_key
  ,cast(customer_category_key as integer) as customer_category_key
  
from 
  rename_source_dim_customer 
)


,dim_customer__convert_boolean as (
SELECT 
  *
  ,case
    when is_on_credit_hold_boolean is true then 'On Credit Hold'
    when is_on_credit_hold_boolean is false then 'Not On Credit Hold'
    when is_on_credit_hold_boolean is null then 'Undefined'
    else 'Invalid' end as is_on_credit_hold
  ,case
    when is_statement_sent_boolean is true then 'Statement Sent'
    when is_statement_sent_boolean is false then 'Statement Not Sent'
    when is_statement_sent_boolean is null then 'Undefined'
    else 'Invalid' end as is_statement_sent
FROM
  cast_source_dim_customer
)

select 
  dim_customer.customer_key
  ,dim_customer.customer_name
  ,dim_customer.bill_to_customer_key
  ,dim_customer.credit_limit
  ,dim_customer.account_opened_date
  ,dim_customer.standard_discount_percentage
  ,dim_customer.is_statement_sent
  ,dim_customer.payment_days
  ,dim_customer.phone_number
  ,dim_customer.fax_number
  ,dim_customer.delivery_run
  ,dim_customer.run_position
  ,dim_customer.primary_contact_person_key
  ,coalesce (dim_person_primary.full_name,'Invalid') as primary_contact_person_name
  ,dim_customer.alternate_contact_person_key
  ,coalesce (dim_person_alternate.full_name,'Invalid') as alternate_contact_person_name
  ,dim_customer.delivery_method_key
  ,coalesce (dim_delivery_method.delivery_method_name, 'Invalid') as delivery_method_name
  ,dim_customer.delivery_city_key
  ,coalesce(dim_city_delivery.city_name ,'Invalid') as delivery_city_name
  ,coalesce(dim_city_delivery.state_province_key ,-1) as delivery_state_province_key
  ,coalesce(dim_city_delivery.state_province_name ,'Invalid') as delivery_state_province_name
  ,dim_customer.postal_city_key
  ,coalesce(dim_city_postal.city_name,'Invalid') as postal_city_name
  ,coalesce(dim_city_postal.state_province_key,-1) as postal_state_province_key
  ,coalesce(dim_city_postal.state_province_name,'Invalid') as postal_state_province_name
  ,dim_customer.customer_category_key
  ,coalesce (dim_customer_category.customer_category_name,'Invalid') as customer_category_name
  ,dim_customer.buying_group_key
  ,coalesce (dim_buying_group.buying_group_name,'Invalid') as buying_group_name
  ,dim_customer.is_on_credit_hold

FROM dim_customer__convert_boolean dim_customer

left join {{ref('dim_person')}} as dim_person_primary
on dim_customer.primary_contact_person_key=dim_person_primary.person_key

left join {{ref('dim_person')}} as dim_person_alternate
on dim_customer.alternate_contact_person_key=dim_person_alternate.person_key

left join {{ref('dim_delivery_method')}} as dim_delivery_method
on dim_customer.delivery_method_key=dim_delivery_method.delivery_method_key

left join {{ref('dim_city')}} as dim_city_delivery
on dim_customer.delivery_city_key=dim_city_delivery.city_key

left join {{ref('dim_city')}} as dim_city_postal
on dim_customer.postal_city_key=dim_city_postal.city_key

left join {{ref('stg_dim_buying_groups')}} as dim_buying_group
on dim_customer.buying_group_key=dim_buying_group.buying_group_key

left join {{ref('stg_dim_customer_categories')}} as dim_customer_category
on dim_customer.customer_category_key=dim_customer_category.customer_category_key


