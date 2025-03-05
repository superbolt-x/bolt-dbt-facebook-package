{%- macro get_facebook_campaigns_insights__child_source(table_name) -%}


{%- set action_types = dbt_utils.get_column_values(source('facebook_raw','campaigns_insights_'~table_name),'action_type') -%}
{%- set attributions = ['_1_d_view','_7_d_click'] -%}

SELECT 
    date,
    ad_id,
    {% for action_type in action_types -%}
    {%- set alias = conversion_alias_config(action_type~table_name.split('action')[1].split('s')[0]) if 'action' in table_name else conversion_alias_config(action_type~table_name.split('conversion')[1].split('s')[0]) -%}
    {%- if alias|length %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN value ELSE 0 END), 0) as {{alias}},
        {%- for attribution in attributions %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN "{{attribution}}" ELSE 0 END), 0) as {{alias}}{{attribution}}
        {%- if not loop.last %},{% endif %}
        {%- endfor -%}
    {%- else -%}
        {%- if 'action' in table_name %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN value ELSE 0 END), 0) as "{{action_type}}{{table_name.split('action')[1].split('s')[0]}}",
        {%- for attribution in attributions %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN "{{attribution}}" ELSE 0 END), 0) as "{{action_type}}{{table_name.split('action')[1].split('s')[0]}}{{attribution}}"
        {%- if not loop.last %},{% endif -%}
        {%- endfor -%}
        {%- elif 'conversion' in table_name %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN value ELSE 0 END), 0) as "{{action_type}}{{table_name.split('conversion')[1].split('s')[0]}}",
        {%- for attribution in attributions %}
        COALESCE(SUM(CASE WHEN action_type = '{{action_type}}' THEN "{{attribution}}" ELSE 0 END), 0) as "{{action_type}}{{table_name.split('conversion')[1].split('s')[0]}}{{attribution}}"
        {%- if not loop.last %},{% endif -%}
        {%- endfor -%}
        {%- endif -%}
    {%- endif -%}
    {%- if not loop.last %},{%- endif %}

{% endfor %}

    FROM {{ source('facebook_raw','campaigns_insights_'~table_name) }}

    GROUP BY 1,2

{%- endmacro %}
