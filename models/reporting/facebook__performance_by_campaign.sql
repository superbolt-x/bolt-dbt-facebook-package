{{ config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

{%- set currency_fields = ["spend", "revenue"] -%}
{%- set exclude_fields = [
    "_fivetran_id","_fivetran_synced","account_name","account_currency",
    "campaign_name","inline_link_clicks","offsite_conversion.fb_pixel_add_payment_info",
    "add_payment_info","offsite_conversion.fb_pixel_view_content","view_content",
    "omni_view_content","offsite_conversion.fb_pixel_view_content_value","omni_view_content_value",
    "lead","leadgen_grouped","omni_add_to_cart","web_add_to_cart","add_to_cart_value",
    "omni_add_to_cart_value","web_add_to_cart_value","omni_initiated_checkout",
    "web_initiate_checkout","omni_initiated_checkout_value","omni_purchase",
    "web_purchases","omni_purchase_value"
] -%}
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set dimensions = ['account_id','campaign_id','attribution_setting'] -%}

-- ===========================================================
-- 1️⃣ RAW STAGE (was stg_campaigns_insights)
-- ===========================================================
with insights_source as (
    select * from {{ source('facebook_raw','campaigns_insights') }}
),

actions_source as (
    {{ get_facebook_campaigns_insights__child_source('actions') }}
)

{% if check_source_exists('facebook_raw','campaigns_insights_conversions') %}
,conversions_source as (
    {{ get_facebook_campaigns_insights__child_source('conversions') }}
)
{% endif %}

{% if check_source_exists('facebook_raw','campaigns_insights_action_values') %}
,action_values_source as (
    {{ get_facebook_campaigns_insights__child_source('action_values') }}
)
{% endif %}

{% if check_source_exists('facebook_raw','campaigns_insights_conversion_values') %}
,conversion_values_source as (
    {{ get_facebook_campaigns_insights__child_source('conversion_values') }}
)
{% endif %}

{% if check_source_exists('facebook_raw','campaigns_insights_catalog_segment_actions') %}
,segment_actions_source as (
    {{ get_facebook_campaigns_insights__segment_source('catalog_segment_actions') }}
)
{% endif %}

{% if check_source_exists('facebook_raw','campaigns_insights_catalog_segment_value') %}
,segment_value_source as (
    {{ get_facebook_campaigns_insights__segment_source('catalog_segment_value') }}
)
{% endif %}

,raw_insights as (
    select 
        i.*,
        max(i._fivetran_synced) over (partition by i.account_name) as last_updated,
        i.campaign_id || '_' || i.date as unique_key
    from insights_source i
    left join actions_source using(date, campaign_id)
    {% if check_source_exists('facebook_raw','campaigns_insights_conversions') %}
        left join conversions_source using(date, campaign_id)
    {% endif %}
    {% if check_source_exists('facebook_raw','campaigns_insights_action_values') %}
        left join action_values_source using(date, campaign_id)
    {% endif %}
    {% if check_source_exists('facebook_raw','campaigns_insights_conversion_values') %}
        left join conversion_values_source using(date, campaign_id)
    {% endif %}
    {% if check_source_exists('facebook_raw','campaigns_insights_catalog_segment_actions') %}
        left join segment_actions_source using(date, campaign_id)
    {% endif %}
    {% if check_source_exists('facebook_raw','campaigns_insights_catalog_segment_value') %}
        left join segment_value_source using(date, campaign_id)
    {% endif %}
    {% if is_incremental() %}
        where i.date >= (select max(date) - 7 from {{ this }})
    {% endif %}
),

-- ===========================================================
-- 2️⃣ CURRENCY NORMALIZATION (was campaigns_insights)
-- ===========================================================
{% if var('currency') != 'USD' %}
currency as (
    select distinct
        date,
        "{{ var('currency') }}" as raw_rate,
        lag(raw_rate) ignore nulls over (order by date) as exchange_rate
    from utilities.dates
    left join utilities.currency using(date)
    where date <= current_date
),
{% endif %}

clean_insights as (
    select
        {% set exchange_rate = 1 if var('currency') == 'USD' else 'exchange_rate' %}
        {% set stg_fields = adapter.get_columns_in_relation(source('facebook_raw','campaigns_insights'))
            | map(attribute="name")
            | reject("in",exclude_fields)
        %}
        {% for field in stg_fields if (("_1_d_view" not in field and "_7_d_click" not in field) or ("purchases" in field or "revenue" in field)) %}
            {% if field in currency_fields or '_value' in field %}
                "{{ field }}"::float/{{ exchange_rate }} as "{{ field }}"
            {% else %}
                "{{ field }}"
            {% endif %}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    from raw_insights
    {% if var('currency') != 'USD' %}
        left join currency using(date)
    {% endif %}
),

insights_stg as (
    select *,
        {{ get_date_parts('date') }}
    from clean_insights
),

-- ===========================================================
-- 3️⃣ AGGREGATION (was performance_by_campaign)
-- ===========================================================
campaigns_meta as (
    select
        {{ get_facebook_clean_field('campaigns','id') }},
        {{ get_facebook_clean_field('campaigns','name') }},
        {{ get_facebook_clean_field('campaigns','daily_budget') }},
        {{ get_facebook_clean_field('campaigns','effective_status') }},
        {{ get_facebook_clean_field('campaigns','account_id') }},
        max(updated_time) over (partition by id) as last_updated_time
    from {{ source('facebook_raw','campaigns') }}
),

{% for date_granularity in date_granularity_list %}
performance_{{date_granularity}} as (
    select
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        {% for dim in dimensions %}{{ dim }},{% endfor %}
        {% set measures = adapter.get_columns_in_relation(source('facebook_raw','campaigns_insights'))
            | map(attribute="name")
            | reject("in", ['date','day','week','month','quarter','year','last_updated','unique_key'])
            | reject("in",dimensions)
            | list
        %}
        {% for m in measures %}
            coalesce(sum("{{ m }}"),0) as "{{ m }}"
            {% if not loop.last %},{% endif %}
        {% endfor %}
    from insights_stg
    group by {{ range(1, dimensions|length + 2 + 1)|list|join(',') }}
),
{% endfor %}

-- ===========================================================
-- 4️⃣ FINAL OUTPUT
-- ===========================================================
final as (
    {% for date_granularity in date_granularity_list %}
        select * from performance_{{date_granularity}}
        {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select
    f.*,
    {{ get_facebook_default_campaign_types('campaign_name') }}
from final f
left join campaigns_meta using(account_id, campaign_id);
