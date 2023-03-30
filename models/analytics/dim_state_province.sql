
with dim_state_province__source as (

  SELECT 
  *
  FROM  `vit-lam-data.wide_world_importers.application__state_provinces`
)

, dim_state_province__rename_column as (
  SELECT 
    state_province_id as state_province_key
    ,state_province_code as state_province_code
    ,state_province_name as state_province_name 
    ,country_id as country_key
  from dim_state_province__source
) 

, dim_state_province__cast_type as (
  SELECT
    cast(state_province_key as integer) as state_province_key
    ,cast(state_province_code as string) as state_province_code
    ,cast(state_province_name as string) as state_province_name
    ,cast(country_key as int) as country_key
  from dim_state_province__rename_column
)

,dim_state_province_add_undefined_record as (
SELECT
  state_province_key
  ,state_province_code
  ,state_province_name
  ,country_key
from dim_state_province__cast_type

union all

SELECT 
  0 as state_province_key
  ,'Undefined' as state_province_code
  ,'Undefined' as state_province_name
  ,0 as country_key

union all

SELECT 
  -1 as state_province_key
  ,'Error' as state_province_code
  ,'Error' as state_province_name
  ,-1 as country_key
)

SELECT
  dim_state.state_province_key
  ,dim_state.state_province_code
  ,dim_state.state_province_name
  ,dim_state.country_key
  ,coalesce (dim_country.country_name,'Invalid') as country_name
from 
  dim_state_province_add_undefined_record as dim_state
left join {{ref('dim_country')}} as dim_country
on dim_state.country_key = dim_country.country_key 
