{{ config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

-- ===========================================================
-- 1️⃣ SOURCES
-- ===========================================================

with insights as (
    select 
        date,
        campaign_id,
        account_id,
        attribution_setting,
        sum(spend) as spend,
        sum(impressions) as impressions,
        sum(clicks) as clicks
    from {{ source('facebook_raw','campaigns_insights') }}
    {% if is_incremental() %}
        where date >= (select max(date) - 7 from {{ this }})
    {% endif %}
    group by 1,2,3,4
),

actions as (
    select 
        date,
        campaign_id,
        sum(purchases) as purchases,
        sum(add_to_cart) as add_to_cart
    from {{ get_facebook_campaigns_insights__child_source('actions') }}
    group by 1,2
),

conversion_values as (
    select 
        date,
        campaign_id,
        sum(purchase_value) as revenue
    from {{ get_facebook_campaigns_insights__child_source('conversion_values') }}
    group by 1,2
),

-- ===========================================================
-- 2️⃣ JOIN SAFE (1:1)
-- ===========================================================

joined as (
    select 
        i.date,
        i.campaign_id,
        i.account_id,
        i.attribution_setting,

        i.spend,
        i.impressions,
        i.clicks,

        coalesce(a.purchases, 0) as purchases,
        coalesce(a.add_to_cart, 0) as add_to_cart,
        coalesce(cv.revenue, 0) as revenue,

        i.campaign_id || '_' || i.date as unique_key

    from insights i
    left join actions a 
        on i.campaign_id = a.campaign_id
        and i.date = a.date

    left join conversion_values cv 
        on i.campaign_id = cv.campaign_id
        and i.date = cv.date
),

-- ===========================================================
-- 3️⃣ METADATA
-- ===========================================================

campaigns_meta as (
    select
        id as campaign_id,
        name as campaign_name,
        account_id,
        max(updated_time) as last_updated_time
    from {{ source('facebook_raw','campaigns') }}
    group by 1,2,3
)

-- ===========================================================
-- 4️⃣ FINAL
-- ===========================================================

select
    j.*,
    m.campaign_name

from joined j
left join campaigns_meta m 
    on j.campaign_id = m.campaign_id
    and j.account_id = m.account_id
