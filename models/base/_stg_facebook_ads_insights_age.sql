{%- set source_relation = adapter.get_relation(
      database=source('facebook_raw', 'ads_insights_age').database,
      schema=source('facebook_raw', 'ads_insights_age').schema,
      identifier=source('facebook_raw', 'ads_insights_age').name) -%}

{% set table_exists=source_relation is not none   %}

{% if table_exists %}

{{ config( 
        materialized='incremental',
        unique_key='unique_key',
        on_schema_change='append_new_columns'
) }}

{%- set schema_name, table_name = 'facebook_raw', 'ads_insights_age' -%}

with insights_source as (

    SELECT * 
    FROM {{ source(schema_name, table_name) }}

    ),

    actions_source as (

    {{ get_facebook_ads_insights_age__child_source('age_actions') }}

    )
    
    {%- set conversions_table_exists = check_source_exists('facebook_raw','ads_insights_age_conversions') %}
    {%- if not conversions_table_exists %}

    {%- else %}
    ,conversions_source as (

    {{ get_facebook_ads_insights_age__child_source('age_conversions') }}

    )
    {%- endif %}
    
    {%- set action_values_table_exists = check_source_exists('facebook_raw','ads_insights_age_action_values') %}
    {%- if not action_values_table_exists %}

    {%- else %}
    ,action_values_source as (

    {{ get_facebook_ads_insights_age__child_source('age_action_values') }}

    )
    {%- endif %}

    {%- set conversion_values_table_exists = check_source_exists('facebook_raw','ads_insights_age_conversion_values') %}
    {%- if not conversion_values_table_exists %}

    {%- else %}
    ,conversion_values_source as (

    {{ get_facebook_ads_insights_age__child_source('age_conversion_values') }}

    )
    {%- endif %}

SELECT 
    *,
    MAX(_fivetran_synced) over (PARTITION BY account_name) as last_updated,
    ad_id||'_'||date||'_'||age as unique_key

FROM insights_source 
LEFT JOIN actions_source USING(date, ad_id)
{%- if not conversions_table_exists %}
{%- else %}
LEFT JOIN conversions_source USING(date, ad_id)
{%- endif %}
{%- if not action_values_table_exists %}
{%- else %}
LEFT JOIN action_values_source USING(date, ad_id)
{%- endif %}
{%- if not conversion_values_table_exists %}
{%- else %}
LEFT JOIN conversion_values_source USING(date, ad_id)
{%- endif %}

{% if is_incremental() -%}

  -- this filter will only be applied on an incremental run
where date >= (select max(date)-7 from {{ this }})

{% endif %}


{% else %}

select
    null::varchar as ad_id,
    null::varchar as age,
    null::date as date,
    null::varchar as _fivetran_id

-- this means there will be zero rows
where false

{% endif %}
