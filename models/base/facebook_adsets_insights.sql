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

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_facebook_adsets_insights'))
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
    FROM {{ ref('_stg_facebook_adsets_insights') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    )

SELECT *,
    {{ get_date_parts('date') }}
FROM insights 
