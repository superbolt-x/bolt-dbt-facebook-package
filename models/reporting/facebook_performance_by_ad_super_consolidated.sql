{{ config (
    alias = target.database + '_facebook_performance_by_ad_super_consolidated',
    materialized = 'incremental',
    unique_key = 'ad_id_date_granularity_key',
    on_schema_change = 'append_new_columns'
)}}

{%- set currency_fields = [
    "spend",
    "revenue"
]
-%}

{%- set exclude_fields = [
    "_fivetran_id",
    "_fivetran_synced",
    "account_name",
    "account_currency",
    "campaign_name",
    "adset_name",
    "ad_name",
    "inline_link_clicks",
    "offsite_conversion.fb_pixel_view_content",
    "view_content",
    "omni_view_content",
    "offsite_conversion.fb_pixel_view_content_value",
    "omni_view_content_value",
    "lead",
    "leadgen_grouped",
    "omni_add_to_cart",
    "web_add_to_cart",
    "add_to_cart_value",
    "omni_add_to_cart_value",
    "web_add_to_cart_value",
    "omni_initiated_checkout",
    "web_initiate_checkout",
    "omni_initiated_checkout_value",
    "omni_purchase",
    "web_purchases",
    "omni_purchase_value"
]
-%}

WITH 
    -- Direct source query from facebook_raw without intermediate staging tables
    insights_source AS (
        SELECT * 
        FROM {{ source('facebook_raw', 'ads_insights') }}
    ),

    actions_source AS (
        {{ get_facebook_ads_insights__child_source('actions') }}
    ),
    
    {%- set conversions_table_exists = check_source_exists('facebook_raw','ads_insights_conversions') %}
    {%- if not conversions_table_exists %}
    {%- else %}
    conversions_source AS (
        {{ get_facebook_ads_insights__child_source('conversions') }}
    ),
    {%- endif %}
    
    {%- set action_values_table_exists = check_source_exists('facebook_raw','ads_insights_action_values') %}
    {%- if not action_values_table_exists %}
    {%- else %}
    action_values_source AS (
        {{ get_facebook_ads_insights__child_source('action_values') }}
    ),
    {%- endif %}

    {%- set conversion_values_table_exists = check_source_exists('facebook_raw','ads_insights_conversion_values') %}
    {%- if not conversion_values_table_exists %}
    {%- else %}
    conversion_values_source AS (
        {{ get_facebook_ads_insights__child_source('conversion_values') }}
    ),
    {%- endif %}
        
    {%- set segment_actions_table_exists = check_source_exists('facebook_raw','ads_insights_catalog_segment_actions') %}
    {%- if not segment_actions_table_exists %}
    {%- else %}
    segment_actions_source AS (
        {{ get_facebook_ads_insights__segment_source('catalog_segment_actions') }}
    ),
    {%- endif %}
        
    {%- set segment_value_table_exists = check_source_exists('facebook_raw','ads_insights_catalog_segment_value') %}
    {%- if not segment_value_table_exists %}
    {%- else %}
    segment_value_source AS (
        {{ get_facebook_ads_insights__segment_source('catalog_segment_value') }}
    ),
    {%- endif %}

    {% if var('currency') != 'USD' -%}
    currency AS (
        SELECT DISTINCT date, "{{ var('currency') }}" as raw_rate, 
            LAG(raw_rate) ignore nulls over (order by date) as exchange_rate
        FROM utilities.dates 
        LEFT JOIN utilities.currency USING(date)
        WHERE date <= current_date
    ),
    {%- endif -%}

    {%- set exchange_rate = 1 if var('currency') == 'USD' else 'exchange_rate' %}
    
    -- Combine all insights data directly into one CTE
    combined_insights AS (
        SELECT 
            insights_source.*,
            -- Join actions and conversions
            actions_source.*
            {%- if not conversions_table_exists %}
            {%- else %},
            conversions_source.*
            {%- endif %}
            {%- if not action_values_table_exists %}
            {%- else %},
            action_values_source.*
            {%- endif %}
            {%- if not conversion_values_table_exists %}
            {%- else %},
            conversion_values_source.*
            {%- endif %}
            {%- if not segment_actions_table_exists %}
            {%- else %},
            segment_actions_source.*
            {%- endif %}
            {%- if not segment_value_table_exists %}
            {%- else %},
            segment_value_source.*
            {%- endif %}
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
        {%- if var('currency') != 'USD' %}
        LEFT JOIN currency USING(date)
        {%- endif %}
        {% if is_incremental() %}
        -- Only process new data when running incrementally
        WHERE insights_source.date >= (SELECT MAX(date) - 500 FROM {{ this }})
        {% endif %}
    ),

    -- Apply currency conversion and extract date parts in one step
    insights_processed AS (
        SELECT 
            combined_insights.ad_id,
            combined_insights.campaign_id,
            combined_insights.adset_id,
            combined_insights.account_id,
            combined_insights.attribution_setting,
            combined_insights.date,
            {{ get_date_parts('combined_insights.date') }},
            
            -- Apply currency conversion dynamically to all needed fields
            {% for field in currency_fields -%}
            combined_insights."{{ field }}"::float/{{ exchange_rate }} as "{{ field }}",
            {% endfor -%}
            
            -- Include all other non-currency fields without modification
            {% for col in adapter.get_columns_in_relation(source('facebook_raw', 'ads_insights')) -%}
            {% if col.name not in exclude_fields and col.name not in currency_fields and col.name not in ['ad_id', 'campaign_id', 'adset_id', 'account_id', 'attribution_setting', 'date'] -%}
            combined_insights."{{ col.name }}",
            {% endif -%}
            {% endfor -%}

            -- Include value fields with currency conversion 
            {% for col in adapter.get_columns_in_relation(source('facebook_raw', 'ads_insights')) -%}
            {% if '_value' in col.name and col.name not in exclude_fields and col.name not in currency_fields -%}
            combined_insights."{{ col.name }}"::float/{{ exchange_rate }} as "{{ col.name }}",
            {% endif -%}
            {% endfor -%}
            
            -- Include remaining fields from joined tables
            {% for table in ['actions_source', 'conversions_source', 'action_values_source', 'conversion_values_source', 'segment_actions_source', 'segment_value_source'] -%}
            {% if table in ['conversions_source', 'action_values_source', 'conversion_values_source', 'segment_actions_source', 'segment_value_source'] -%}
            {% set table_exists = true if table == 'actions_source' else conversions_table_exists if table == 'conversions_source' else action_values_table_exists if table == 'action_values_source' else conversion_values_table_exists if table == 'conversion_values_source' else segment_actions_table_exists if table == 'segment_actions_source' else segment_value_table_exists -%}
            {% if table_exists -%}
            -- Fields from {{ table }}
            {% endif -%}
            {% endif -%}
            {% endfor -%}
            
            1 as dummy_field  -- Placeholder to handle trailing commas in generated SQL
        FROM combined_insights
    ),

    -- Get ads, adsets, and campaigns in parallel
    ads AS (
        SELECT
            id::bigint as ad_id,
            name as ad_name,
            effective_status as ad_effective_status,
            account_id,
            ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_time DESC) as rn
        FROM {{ source('facebook_raw', 'ads') }}
        WHERE 1=1 -- Will be filtered in the query below
    ),
    
    ads_latest AS (
        SELECT
            ad_id,
            ad_name,
            ad_effective_status,
            account_id
        FROM ads
        WHERE rn = 1
    ),
    
    adsets AS (
        SELECT
            id::bigint as adset_id,
            name as adset_name,
            effective_status as adset_effective_status,
            account_id,
            ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_time DESC) as rn
        FROM {{ source('facebook_raw', 'adsets') }}
        WHERE 1=1 -- Will be filtered in the query below
    ),
    
    adsets_latest AS (
        SELECT
            adset_id,
            adset_name,
            adset_effective_status,
            account_id
        FROM adsets
        WHERE rn = 1
    ),
    
    campaigns AS (
        SELECT
            id::bigint as campaign_id,
            name as campaign_name,
            effective_status as campaign_effective_status,
            account_id,
            ROW_NUMBER() OVER(PARTITION BY id ORDER BY updated_time DESC) as rn
        FROM {{ source('facebook_raw', 'campaigns') }}
        WHERE 1=1 -- Will be filtered in the query below
    ),
    
    campaigns_latest AS (
        SELECT
            campaign_id,
            campaign_name,
            campaign_effective_status,
            account_id
        FROM campaigns
        WHERE rn = 1
    ),

    {%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
    {%- set dimensions = ['account_id','campaign_id','adset_id','ad_id','attribution_setting'] -%}
    {%- set measures = adapter.get_columns_in_relation(ref('facebook_ads_insights'))
                        |map(attribute="name")
                        |reject("in",exclude_fields)
                        |reject("in",dimensions)
                        |reject("in",['day','week','month','quarter','year'])
                        |list
                        -%}

    -- Aggregate data at different date granularities in a single pass
    aggregated_performance AS (
        {% for date_granularity in date_granularity_list -%}
        SELECT 
            '{{date_granularity}}' as date_granularity,
            {{date_granularity}} as date,
            {%- for dimension in dimensions %}
            insights_processed.{{ dimension }},
            {%- endfor %}
            {% for measure in measures -%}
            COALESCE(SUM(insights_processed."{{ measure }}"),0) as "{{ measure }}"
            {%- if not loop.last %},{%- endif %}
            {% endfor %}
        FROM insights_processed
        GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}
        
        {% if not loop.last %}UNION ALL
        {% endif %}
        {%- endfor %}
    )

-- Final result with dimension attributes and scoring objects
SELECT 
    perf.*,
    ad.ad_name,
    ad.ad_effective_status,
    adset.adset_name,
    adset.adset_effective_status,
    campaign.campaign_name,
    campaign.campaign_effective_status,
    {{ get_facebook_default_campaign_types('campaign.campaign_name')}},
    {{ get_facebook_scoring_objects() }},
    -- Create a unique key for incremental processing
    perf.ad_id::varchar || '_' || perf.date || '_' || perf.date_granularity as ad_id_date_granularity_key
FROM aggregated_performance perf
LEFT JOIN ads_latest ad ON perf.account_id = ad.account_id AND perf.ad_id::varchar = ad.ad_id::varchar
LEFT JOIN adsets_latest adset ON perf.account_id = adset.account_id AND perf.adset_id = adset.adset_id
LEFT JOIN campaigns_latest campaign ON perf.account_id = campaign.account_id AND perf.campaign_id = campaign.campaign_id