{{ config (
    alias = target.database + '_facebook_performance_by_ad_consolidated_bis'

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

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_facebook_ads_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

WITH 
    {% if var('currency') != 'USD' -%}
    currency AS
    (SELECT DISTINCT date, "{{ var('currency') }}" as raw_rate, 
        LAG(raw_rate) ignore nulls over (order by date) as exchange_rate
    FROM utilities.dates 
    LEFT JOIN utilities.currency USING(date)
    WHERE date <= current_date),
    {%- endif -%}

    {%- set exchange_rate = 1 if var('currency') == 'USD' else 'exchange_rate' %}
    
    insights AS 
    (SELECT 
        {%- for field in stg_fields if (("_1_d_view" not in field and "_7_d_click" not in field) or ("purchases" in field or "revenue" in field)) -%}
        {%- if field in currency_fields or '_value' in field %}
        "{{ field }}"::float/{{ exchange_rate }} as "{{ field }}"
        {%- else %}
        "{{ field }}"
        {%- endif -%}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ ref('_stg_facebook_ads_insights') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    ),

    insights_stg AS 
    (SELECT *,
    {{ get_date_parts('date') }}
    FROM insights),


{%- set selected_fields = [
    "id",
    "name",
    "effective_status",
    "account_id",
    "updated_time"
] -%}
{%- set schema_name, table_name = 'facebook_raw', 'ads' -%}

    ads_staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_facebook_clean_field(table_name, field) }},
        {% endfor -%}
        MAX(updated_time) OVER (PARTITION BY id) as last_updated_time

    FROM {{ source(schema_name, table_name) }}
    ),

    ads AS
    (SELECT *,
        ad_id as unique_key
    FROM ads_staging 
    WHERE updated_time = last_updated_time),

{%- set selected_fields = [
    "id",
    "name",
    "billing_event",
    "bid_strategy",
    "daily_budget",
    "effective_status",
    "optimization_goal",
    "account_id",
    "updated_time"
] -%}
{%- set schema_name, table_name = 'facebook_raw', 'adsets' -%}

    adsets_staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_facebook_clean_field(table_name, field) }},
        {% endfor -%}
        MAX(updated_time) OVER (PARTITION BY id) as last_updated_time

    FROM {{ source(schema_name, table_name) }}
    ),

    adsets AS
    (SELECT *,
        adset_id as unique_key
    FROM adsets_staging 
    WHERE updated_time = last_updated_time),

{%- set selected_fields = [
    "id",
    "name",
    "daily_budget",
    "effective_status",
    "account_id",
    "updated_time"
] -%}
{%- set schema_name, table_name = 'facebook_raw', 'campaigns' -%}

    campaigns_staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_facebook_clean_field(table_name, field) }},
        {% endfor -%}
        MAX(updated_time) OVER (PARTITION BY id) as last_updated_time

    FROM {{ source(schema_name, table_name) }}
    ),

    campaigns AS
    (SELECT *,
        campaign_id as unique_key
    FROM campaigns_staging 
    WHERE updated_time = last_updated_time),

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key'] -%}
{%- set dimensions = ['account_id','campaign_id','adset_id','ad_id','attribution_setting'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('facebook_ads_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |reject("in",dimensions)
                    |list
                    -%}  
 
    {%- for date_granularity in date_granularity_list %}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        {%- for dimension in dimensions %}
        {{ dimension }},
        {%-  endfor %}
        {% for measure in measures -%}
        COALESCE(SUM("{{ measure }}"),0) as "{{ measure }}"
        {%- if not loop.last %},{%- endif %}
        {% endfor %}
    FROM {{ ref('facebook_ads_insights') }}
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}),
    {%- endfor %}

SELECT *,
    {{ get_facebook_default_campaign_types('campaign_name')}},
    {{ get_facebook_scoring_objects() }}
FROM 
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
LEFT JOIN ads USING(account_id,ad_id)
LEFT JOIN adsets USING(account_id,adset_id)
LEFT JOIN campaigns USING(account_id,campaign_id)
