{{ config (
    alias = target.database + '_facebook_performance_by_ad'
) }}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key'] -%}
{%- set dimensions = ['account_id','campaign_id','adset_id','ad_id','attribution_setting'] -%}

-- facebook_ads_insights
{%- set currency_fields = [ "spend", "revenue" ] -%}
{%- set exclude_fields = [ "_fivetran_id", "_fivetran_synced", "account_name", "account_currency", "campaign_name", "adset_name", "ad_name", "inline_link_clicks", "offsite_conversion.fb_pixel_view_content", "view_content", "omni_view_content", "offsite_conversion.fb_pixel_view_content_value", "omni_view_content_value", "lead", "leadgen_grouped", "omni_add_to_cart", "web_add_to_cart", "add_to_cart_value", "omni_add_to_cart_value", "web_add_to_cart_value", "omni_initiated_checkout", "web_initiate_checkout", "omni_initiated_checkout_value", "omni_purchase", "web_purchases", "omni_purchase_value" ] -%}
{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_facebook_ads_insights')) | map(
    attribute = "name"
) | reject(
    "in",
    exclude_fields
) -%}
WITH {% if var('currency') != 'USD' -%}
    currency AS (

        SELECT
            DISTINCT DATE,
            "{{ var('currency') }}" AS raw_rate,
            LAG(raw_rate) ignore nulls over (
                ORDER BY
                    DATE
            ) AS exchange_rate
        FROM
            utilities.dates
            LEFT JOIN utilities.currency USING(DATE)
        WHERE
            DATE <= CURRENT_DATE
    ),
{%- endif -%}

{%- set exchange_rate = 1 if var('currency') == 'USD' else 'exchange_rate' %}
insights AS (
    SELECT
        {%- for field in stg_fields if (
                (
                    "_1_d_view" not in field and "_7_d_click" not in field
                ) or (
                    "purchases" in field or "revenue" in field
                )
            ) -%}
            {%- if field in currency_fields or '_value' in field %}
                "{{ field }}" :: FLOAT / {{ exchange_rate }} AS "{{ field }}"
            {%- else %}
                "{{ field }}"
            {%- endif -%}

            {%- if not loop.last %},
            {%- endif %}
        {%- endfor %}
    FROM
        {{ ref('_stg_facebook_ads_insights') }}

        {%- if var('currency') != 'USD' %}
            LEFT JOIN currency USING(DATE)
        {%- endif %}
),
facebook_ads_insights AS (
    SELECT
        *,
        {{ get_date_parts('date') }}
    FROM
        insights
),
{%- set measures = adapter.get_columns_in_relation(facebook_ads_insights)
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |reject("in",dimensions)
                    |list
                    -%}

-- facebook_ads
{%- set selected_fields = [
    "id",
    "name",
    "effective_status",
    "account_id",
    "updated_time"
] -%}
{%- set schema_name, table_name = 'facebook_raw', 'ads' -%}
staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_facebook_clean_field(table_name, field) }},
        {% endfor -%}
        MAX(updated_time) OVER (PARTITION BY id) as last_updated_time

    FROM {{ source(schema_name, table_name) }}
    ),
facebook_ads AS
    (SELECT
        *,
        ad_id AS unique_key
    FROM
        staging
    WHERE
        updated_time = last_updated_time
    ),
    



