{%- macro get_facebook_campaigns_insights__segment_source(table_name) -%}


{%- set action_types = dbt_utils.get_column_values(source('facebook_raw','campaigns_insights_'~table_name),'action_type') -%}
{#- only keep attribution columns Fivetran actually created for this table: the *_values children have no _1_d_view when no view-attributed value ever landed -#}
{%- set available_columns = adapter.get_columns_in_relation(source('facebook_raw','campaigns_insights_'~table_name)) | map(attribute='name') | map('lower') | list -%}
{%- set attributions = ['_1_d_view','_7_d_click'] | select('in', available_columns) | list -%}

SELECT 
    date,
    campaign_id::VARCHAR as campaign_id,
    {% for action_type in action_types -%}
    {%- set alias = conversion_alias_config(action_type) if 'action' in table_name else conversion_alias_config(action_type~'_value') -%}
    {%- if alias|length %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN value ELSE 0 END), 0) as "{{alias}}_with_shared_items",
        {%- for attribution in attributions %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN "{{attribution}}" ELSE 0 END), 0) as "{{alias}}_with_shared_items{{attribution}}"
        {%- if not loop.last %},{% endif %}
        {%- endfor -%}
    {%- else -%}
        {%- if 'action' in table_name %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN value ELSE 0 END), 0) as "{{action_type}}_with_shared_items",
        {%- for attribution in attributions %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN "{{attribution}}" ELSE 0 END), 0) as "{{action_type}}_with_shared_items{{attribution}}"
        {%- if not loop.last %},{% endif -%}
        {%- endfor -%}
        {%- elif 'value' in table_name %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN value ELSE 0 END), 0) as "{{action_type}}_with_shared_items_value",
        {%- for attribution in attributions %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN "{{attribution}}" ELSE 0 END), 0) as "{{action_type}}_with_shared_items_value{{attribution}}"
        {%- if not loop.last %},{% endif -%}
        {%- endfor -%}
        {%- endif -%}
    {%- endif -%}
    {%- if not loop.last %},{%- endif %}

{% endfor %}

    FROM {{ source('facebook_raw','campaigns_insights_'~table_name) }}

    GROUP BY 1,2

{%- endmacro %}
