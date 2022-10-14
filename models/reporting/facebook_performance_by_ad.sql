{{ config (
    alias = target.database + '_facebook_performance_by_ad'
)}}

SELECT *,
    {{ get_date_parts('date') }},
    {{ get_facebook_default_campaign_types('campaign_name')}},
    {{ get_facebook_scoring_objects() }}

FROM {{ ref('facebook_ads_insights') }}
