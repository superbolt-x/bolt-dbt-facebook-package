{{ config( 
        materialized='incremental',
        unique_key='unique_key',
        on_schema_change='append_new_columns'
) }}

{%- set schema_name, table_name = 'facebook_raw', 'ads_insights' -%}

with insights_source as (

    SELECT * 
    FROM {{ source(schema_name, table_name) }}

    ),

    actions_source as (

    {{ get_facebook_ads_insights__child_source('actions') }}

    )
    
    {%- set conversions_table_exists = check_source_exists('facebook_raw','ads_insights_conversions') %}
    {%- if not conversions_table_exists %}

    {%- else %}
    ,conversions_source as (

    {{ get_facebook_ads_insights__child_source('conversions') }}

    )
    {%- endif %}
    
    {%- set action_values_table_exists = check_source_exists('facebook_raw','ads_insights_action_values') %}
    {%- if not action_values_table_exists %}

    {%- else %}
    ,action_values_source as (

    {{ get_facebook_ads_insights__child_source('action_values') }}

    )
    {%- endif %}

    {%- set conversion_values_table_exists = check_source_exists('facebook_raw','ads_insights_conversion_values') %}
    {%- if not conversion_values_table_exists %}

    {%- else %}
    ,conversion_values_source as (

    {{ get_facebook_ads_insights__child_source('conversion_values') }}

    )
    {%- endif %}

        
    {%- set gsheet_segment_actions_table_exists = check_source_exists('gsheet_raw','ads_insights_catalog_segment_actions') %}
    {%- if not gsheet_segment_actions_table_exists %}

    {%- else %}
    ,gsheet_segment_actions_source as (

    {{ get_gsheet_insights__segment_source('catalog_segment_actions') }}

    )
    {%- endif %}

        
    {%- set gsheet_segment_value_table_exists = check_source_exists('gsheet_raw','ads_insights_catalog_segment_value') %}
    {%- if not gsheet_segment_value_table_exists %}

    {%- else %}
    ,gsheet_segment_value_source as (

    {{ get_gsheet_insights__segment_source('catalog_segment_value') }}

    )
    {%- endif %}

    {%- set segment_actions_table_exists = check_source_exists('facebook_raw','ads_insights_catalog_segment_actions') %}
    {%- if not segment_actions_table_exists %}

    {%- else %}
    ,segment_actions_source as (

    {{ get_facebook_ads_insights__segment_source('catalog_segment_actions') }}

    )
    {%- endif %}

        
    {%- set segment_value_table_exists = check_source_exists('facebook_raw','ads_insights_catalog_segment_value') %}
    {%- if not segment_value_table_exists %}

    {%- else %}
    ,segment_value_source as (

    {{ get_facebook_ads_insights__segment_source('catalog_segment_value') }}

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
{%- if not segment_actions_table_exists %}
{%- else %}
LEFT JOIN segment_actions_source USING(date, ad_id)
{%- endif %}
{%- if not segment_value_table_exists %}
{%- else %}
LEFT JOIN segment_value_source USING(date, ad_id)
{%- endif %}
{%- if not gsheet_segment_actions_table_exists %}
{%- else %}
LEFT JOIN gsheet_segment_actions_source USING(date, ad_id)
{%- endif %}
{%- if not gsheet_segment_value_table_exists %}
{%- else %}
LEFT JOIN gsheet_segment_value_source USING(date, ad_id)
{%- endif %}

{% if is_incremental() -%}

  -- this filter will only be applied on an incremental run
where date >= (select max(date)-500 from {{ this }})

{% endif %}

