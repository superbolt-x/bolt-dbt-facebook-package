{{ config (
    alias = target.database + '_facebook_performance_by_ad'
)}}

SELECT *,
    {{ get_date_parts('date') }},
    {{ get_default_campaign_types('campaign_name')}},
    {{ get_scoring_objects() }}

FROM {{ ref('ads_insights') }}
