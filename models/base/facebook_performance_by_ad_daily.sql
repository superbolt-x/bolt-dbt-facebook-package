{{ config(
        materialized='incremental',
        unique_key='unique_key',
        on_schema_change='append_new_columns'
) }}

{#-
    Day-grain staging for the ad performance pipeline.

    This does the same unpacking work as _stg_facebook_ads_insights (joining the
    Fivetran action/conversion child tables into one row per ad x date), but is a
    dedicated model for facebook_performance_by_ad so that model can read an
    incremental day-grain table and roll the coarser grains up from it.

    The unpacked result is already at ad x date grain (Fivetran ads_insights is
    daily per ad), so no aggregation happens here. Incremental, 9-day lookback.
-#}

{%- set schema_name, table_name = 'facebook_raw', 'ads_insights' -%}

with insights_source as (

    SELECT *
    FROM {{ source(schema_name, table_name) }}
    {% if is_incremental() -%}
    -- this filter will only be applied on an incremental run
    -- 9-day lookback covers the 7-day-click (+1-day-view) attribution window with a safety buffer
    WHERE date >= (select max(date)-9 from {{ this }})
    {%- endif %}

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

-- last_updated is a per-account aggregate joined back instead of MAX() OVER (PARTITION BY account_name): the
-- window sent every row of an account to one slice and spilled to disk, and the table inherited KEY(account_name)
-- (skew 100 with one account). insights_source carries the incremental filter, so the aggregate covers the same
-- rows the window did.
,last_updated_source as (

    SELECT account_name, MAX(_fivetran_synced) as last_updated
    FROM insights_source
    GROUP BY 1

    ),

    insights as (

    SELECT *
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

    )

SELECT
    insights.*,
    last_updated_source.last_updated,
    insights.ad_id||'_'||insights.date as unique_key

FROM insights
LEFT JOIN last_updated_source ON insights.account_name = last_updated_source.account_name
