
with dim_person__source as (

  SELECT 
  *
  FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_column as (
  SELECT 
    person_id as person_key
    ,full_name as full_name 
    ,preferred_name 
    ,is_employee 
    ,is_salesperson 
  from dim_person__source
)

, dim_person__cast_type as (
  SELECT
    cast(person_key as integer) as person_key
    ,cast(full_name as string) as full_name
    ,cast(preferred_name as string) as preferred_name
    ,cast(is_employee as boolean) as is_employee_boolean
    ,cast(is_salesperson as boolean) as is_salesperson_boolean
  from dim_person__rename_column
)

,dim_person__convert_boolean as (
SELECT 
  *
  ,case
    when is_employee_boolean is true then 'Employee'
    when is_employee_boolean is false then 'Not Employee'
    when is_employee_boolean is null then 'Undefined'
    else 'Invalid' end as is_employee
  ,case
    when is_salesperson_boolean is true then 'Sales Person Sent'
    when is_salesperson_boolean is false then 'Sales Person Not Sent'
    when is_salesperson_boolean is null then 'Undefined'
    else 'Invalid' end as is_salesperson
FROM
  dim_person__cast_type
)


,dim_person_add_undefined_record as (
SELECT
  person_key
  ,full_name
  ,preferred_name 
  ,is_employee 
  ,is_salesperson
from dim_person__convert_boolean

union all

SELECT 
  0 as person_key
  ,'Undefined' as full_name
  ,'Undefined' as preferred_name 
  ,'Undefined' as is_employee 
  ,'Undefined' as is_salesperson

union all

SELECT 
  -1 as person_key
  ,'Invalid' as full_name
  ,'Invalid' as preferred_name 
  ,'Invalid' as is_employee 
  ,'Invalid' as is_salesperson
)

SELECT
  person_key
  ,full_name
  ,preferred_name
  ,is_employee
  ,is_salesperson
from 
  dim_person_add_undefined_record
