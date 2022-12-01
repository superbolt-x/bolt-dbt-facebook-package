{{ config (
    alias = target.database + '_facebook_performance_by_ad',
    materialized='incremental',
    unique_key='unique_key',
    on_schema_change='append_new_columns'

)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key'] -%}
{%- set dimensions = ['account_id','campaign_id','adset_id','ad_id','attribution_setting'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('facebook_ads_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |reject("in",dimensions)
                    |list
                    -%}  

WITH 
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
    {% if is_incremental() -%}

    -- this filter will only be applied on an incremental run
    WHERE date >= (select max(date)-7 from {{ this }})

    {% endif %}
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}),
    {%- endfor %}

    ads AS 
    (SELECT account_id, ad_id::varchar as ad_id, ad_name, ad_effective_status
    FROM {{ ref('facebook_ads') }}
    ),

    adsets AS 
    (SELECT account_id, adset_id, adset_name, adset_effective_status
    FROM {{ ref('facebook_adsets') }}
    ),

    campaigns AS 
    (SELECT account_id, campaign_id, campaign_name, campaign_effective_status
    FROM {{ ref('facebook_campaigns') }}
    ),

    accounts AS 
    (SELECT account_id, account_name, account_name, account_currency
    FROM {{ ref('facebook_accounts') }} 
    )

SELECT *,
    {{ get_facebook_default_campaign_types('campaign_name')}},
    {{ get_facebook_scoring_objects() }},
    date||'_'||date_granularity||'_'||campaign_name||'_'||adset_name||'_'||ad_name as unique_key
FROM 
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
LEFT JOIN ads USING(account_id, ad_id)
LEFT JOIN adsets USING(account_id, adset_id)
LEFT JOIN campaigns USING(account_id, campaign_id)
