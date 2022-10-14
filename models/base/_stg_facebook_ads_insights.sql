{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'facebook_raw', 'ads_insights' -%}

with insights_source as (

    SELECT * 
    FROM {{ source(schema_name, table_name) }}

    ),

    actions_source as (

    {{ get_facebook_ads_insights__child_source('actions') }}

    )
    
    {%- set conversions_table_exists = check_source_exists('facebook_raw','ads_insights__conversions') %}
    {%- if not conversions_table_exists %}

    {%- else %}
    ,conversions_source as (

    {{ get_facebook_ads_insights__child_source('conversions') }}

    )
    {%- endif %}
    
    {%- set action_values_table_exists = check_source_exists('facebook_raw','ads_insights__action_values') %}
    {%- if not action_values_table_exists %}

    {%- else %}
    ,action_values_source as (

    {{ get_facebook_ads_insights__child_source('action_values') }}

    )
    {%- endif %}

    {%- set conversion_values_table_exists = check_source_exists('facebook_raw','ads_insights__conversion_values') %}
    {%- if not conversion_values_table_exists %}

    {%- else %}
    ,conversion_values_source as (

    {{ get_facebook_ads_insights__child_source('conversion_values') }}

    )
    {%- endif %}

SELECT 
    *,
    MAX(_fivetran_synced) over (PARTITION BY account_name) as last_updated,
    ad_id||'_'||date as unique_key

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

