{{ config (
    alias = target.database + '_facebook_performance_by_campaign'
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
    "inline_link_clicks",
    "offsite_conversion.fb_pixel_add_payment_info",
    "add_payment_info",
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

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_facebook_campaigns_insights'))
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
    FROM {{ ref('_stg_facebook_campaigns_insights') }}
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

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key'] -%}
{%- set dimensions = ['account_id','campaign_id','attribution_setting'] -%}
{%- set measures = ['date','campaign_id','account_id','clicks','impressions','spend','attribution_setting','onsite_web_view_content','onsite_web_app_view_content','page_engagement','post_engagement','link_clicks','landing_page_view','onsite_web_add_to_cart','onsite_web_app_add_to_cart','add_to_cart','post_reaction','onsite_web_app_purchase','onsite_web_purchase','web_in_store_purchase','purchases','purchases_1_d_view','purchases_7_d_click','web_purchases_1_d_view','web_purchases_7_d_click','onsite_web_lead','website_leads','onsite_web_initiate_checkout','initiate_checkout','video_view','post_save','post','comment','offsite_conversion.fb_pixel_custom','onsite_app_view_content','onsite_conversion.view_content','like','onsite_conversion.add_to_wishlist','omni_add_to_wishlist','complete_registration','omni_complete_registration','offsite_conversion.fb_pixel_complete_registration','onsite_app_add_to_cart','onsite_conversion.add_to_cart','onsite_conversion.initiate_checkout','onsite_app_purchase','onsite_conversion.purchase','onsite_conversion.messaging_first_reply','onsite_conversion.messaging_conversation_started_7d','onsite_conversion.messaging_conversation_replied_7d','onsite_conversion.total_messaging_connection','onsite_conversion.messaging_block','subscribe_total','subscribe_website','offsite_conversion.fb_pixel_custom.purchaserechargetest','view_content_value','onsite_web_app_view_content_value','onsite_web_view_content_value','onsite_web_add_to_cart_value','onsite_web_app_add_to_cart_value','onsite_web_app_purchase_value','onsite_web_purchase_value','web_revenue','web_revenue_1_d_view','web_revenue_7_d_click','revenue','revenue_1_d_view','revenue_7_d_click','web_in_store_purchase_value','onsite_web_initiate_checkout_value','initiate_checkout_value','offsite_conversion.fb_pixel_initiate_checkout_value','add_payment_info_value','offsite_conversion.fb_pixel_add_payment_info_value','offsite_conversion.fb_pixel_custom_value','onsite_app_purchase_value','onsite_conversion.purchase_value','subscribe_total_value','subscribe_website_value','offsite_conversion.fb_pixel_custom.purchaserechargetest_value','last_updated','unique_key','web_app_in_store_purchase','omni_landing_page_view','web_app_in_store_purchase_value','day','week','month','quarter','year']
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
    FROM insights_stg
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}),
    {%- endfor %}

    campaigns AS
    (SELECT account_id, campaign_id::varchar as campaign_id, campaign_name, campaign_effective_status
    FROM campaigns_staging 
    WHERE updated_time = last_updated_time)

SELECT *,
    {{ get_facebook_default_campaign_types('campaign_name')}}
FROM 
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
LEFT JOIN campaigns USING(account_id,campaign_id)
