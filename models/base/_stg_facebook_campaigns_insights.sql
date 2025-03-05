{%- set source_relation = adapter.get_relation(
      database=source('facebook_raw', 'campaigns_insights').database,
      schema=source('facebook_raw', 'campaigns_insights').schema,
      identifier=source('facebook_raw', 'campaigns_insights').name) -%}

{% set table_exists=source_relation is not none   %}

{% if table_exists %}

{{ config( 
        materialized='incremental',
        unique_key='unique_key',
        on_schema_change='append_new_columns'
) }}

{%- set schema_name, table_name = 'facebook_raw', 'campaigns_insights' -%}

with insights_source as (

    SELECT * 
    FROM {{ source(schema_name, table_name) }}

    ),
    
    {%- set conversions_table_exists = check_source_exists('facebook_raw','campaigns_insights_conversions') %}
    {%- if not conversions_table_exists %}

    {%- else %}
    ,conversions_source as (

    {{ get_facebook_campaigns_insights_age__child_source('conversions') }}

    )
    {%- endif %}
    
    {%- set action_values_table_exists = check_source_exists('facebook_raw','campaigns_insights_action_values') %}
    {%- if not action_values_table_exists %}

    {%- else %}
    ,action_values_source as (

    {{ get_facebook_campaigns_insights_age__child_source('action_values') }}

    )
    {%- endif %}

    {%- set conversion_values_table_exists = check_source_exists('facebook_raw','campaigns_insights_conversion_values') %}
    {%- if not conversion_values_table_exists %}

    {%- else %}
    ,conversion_values_source as (

    {{ get_facebook_campaigns_insights_age__child_source('conversion_values') }}

    )
    {%- endif %}

SELECT 
    *,
    MAX(_fivetran_synced) over (PARTITION BY account_name) as last_updated,
    campaign_id||'_'||date as unique_key

FROM insights_source 
{%- if not conversions_table_exists %}
{%- else %}
LEFT JOIN conversions_source USING(date, campaign_id, _fivetran_id)
{%- endif %}
{%- if not action_values_table_exists %}
{%- else %}
LEFT JOIN action_values_source USING(date, campaign_id, _fivetran_id)
{%- endif %}
{%- if not conversion_values_table_exists %}
{%- else %}
LEFT JOIN conversion_values_source USING(date, campaign_id, _fivetran_id)
{%- endif %}

{% if is_incremental() -%}

  -- this filter will only be applied on an incremental run
where date >= (select max(date)-7 from {{ this }})

{% endif %}


{% else %}

select
    null::varchar as campaign_id,
    null::date as date,
    null::varchar as _fivetran_id

-- this means there will be zero rows
where false

{% endif %}
