{{ config (
    alias = target.database + '_facebook__performance_by_ad'

)}}

{%- set currency_fields = ["spend", "revenue"] -%}

{%- set exclude_fields = [
    "_fivetran_id","_fivetran_synced","account_name","account_currency",
    "campaign_name","adset_name","ad_name","inline_link_clicks",
    "offsite_conversion.fb_pixel_view_content","view_content","omni_view_content",
    "offsite_conversion.fb_pixel_view_content_value","omni_view_content_value",
    "lead","leadgen_grouped","omni_add_to_cart","web_add_to_cart","add_to_cart_value",
    "omni_add_to_cart_value","web_add_to_cart_value","omni_initiated_checkout",
    "web_initiate_checkout","omni_initiated_checkout_value","omni_purchase",
    "web_purchases","omni_purchase_value"
] -%}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set dimensions = ['account_id','campaign_id','adset_id','ad_id','attribution_setting'] -%}

-- ===========================================================
-- 1️⃣ CLEAN STAGING TABLE
-- ===========================================================
{%- set exchange_rate = 1 if var('currency') == 'USD' else 'exchange_rate' %}

WITH 
{% if var('currency') != 'USD' -%}
currency AS (
    SELECT DISTINCT
        date,
        "{{ var('currency') }}" AS raw_rate,
        LAG(raw_rate) IGNORE NULLS OVER (ORDER BY date) AS exchange_rate
    FROM utilities.dates
    LEFT JOIN utilities.currency USING(date)
    WHERE date <= current_date
),
{% endif %}

insights_stg AS (
    SELECT
        {%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_facebook_ads_insights'))
            | map(attribute="name")
            | reject("in", exclude_fields)
        -%}
        {%- for field in stg_fields %}
            {%- if field in currency_fields or '_value' in field %}
                "{{ field }}"::float/{{ exchange_rate }} AS "{{ field }}"
            {%- else %}
                "{{ field }}"
            {%- endif %}
            {%- if not loop.last %},{% endif %}
        {%- endfor %},
        MAX(_fivetran_synced) OVER (PARTITION BY account_name) AS last_updated,
        ad_id || '_' || date AS unique_key
    FROM {{ ref('_stg_facebook_ads_insights') }}
    {% if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {% endif %}
),

-- ===========================================================
-- 2️⃣ METADATA TABLES
-- ===========================================================
ads_staging AS (
    SELECT
        {{ get_facebook_clean_field('ads','id') }},
        {{ get_facebook_clean_field('ads','name') }},
        {{ get_facebook_clean_field('ads','effective_status') }},
        {{ get_facebook_clean_field('ads','account_id') }},
        MAX(updated_time) OVER (PARTITION BY id) AS last_updated_time
    FROM {{ source('facebook_raw','ads') }}
),

adsets_staging AS (
    SELECT
        {{ get_facebook_clean_field('adsets','id') }},
        {{ get_facebook_clean_field('adsets','name') }},
        {{ get_facebook_clean_field('adsets','effective_status') }},
        {{ get_facebook_clean_field('adsets','account_id') }},
        MAX(updated_time) OVER (PARTITION BY id) AS last_updated_time
    FROM {{ source('facebook_raw','adsets') }}
),

campaigns_staging AS (
    SELECT
        {{ get_facebook_clean_field('campaigns','id') }},
        {{ get_facebook_clean_field('campaigns','name') }},
        {{ get_facebook_clean_field('campaigns','effective_status') }},
        {{ get_facebook_clean_field('campaigns','account_id') }},
        MAX(updated_time) OVER (PARTITION BY id) AS last_updated_time
    FROM {{ source('facebook_raw','campaigns') }}
),

-- ===========================================================
-- 3️⃣ AGGREGATION BY DATE GRANULARITY
-- ===========================================================
{% set exclude_fields_agg = ['date','day','week','month','quarter','year','last_updated','unique_key'] %}

{% for date_granularity in date_granularity_list %}
performance_{{ date_granularity }} AS (
    SELECT
        '{{ date_granularity }}' AS date_granularity,
        {{ date_granularity }} AS date,
        {%- for dim in dimensions %}
            {% if dim == 'ad_id' %}
                CAST({{ dim }} AS BIGINT) AS {{ dim }},
            {% else %}
                {{ dim }},
            {% endif %}
        {%- endfor %}
        {%- for field in stg_fields if field not in exclude_fields_agg and field not in dimensions %}
            COALESCE(SUM("{{ field }}"),0) AS "{{ field }}"
            {%- if not loop.last %},{% endif %}
        {%- endfor %}
    FROM insights_stg
    GROUP BY {{ range(1, dimensions|length + 2 + 1)|list|join(',') }}
)
{% if not loop.last %},{% endif %}
{% endfor %},

-- ===========================================================
-- 4️⃣ FINAL OUTPUT WITH METADATA
-- ===========================================================
ads AS (
    SELECT account_id, ad_id, ad_name, ad_effective_status
    FROM ads_staging
    WHERE updated_time = last_updated_time
),

adsets AS (
    SELECT account_id, adset_id, adset_name, adset_effective_status
    FROM adsets_staging
    WHERE updated_time = last_updated_time
),

campaigns AS (
    SELECT account_id, campaign_id, campaign_name, campaign_effective_status
    FROM campaigns_staging
    WHERE updated_time = last_updated_time
)

SELECT 
    f.*,
    {{ get_facebook_default_campaign_types('campaign_name') }},
    {{ get_facebook_scoring_objects() }}
FROM (
    {% for date_granularity in date_granularity_list %}
    SELECT * FROM performance_{{ date_granularity }}
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
) f
LEFT JOIN ads USING(account_id, ad_id)
LEFT JOIN adsets USING(account_id, adset_id)
LEFT JOIN campaigns USING(account_id, campaign_id)
