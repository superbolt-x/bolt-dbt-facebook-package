{%- macro get_facebook_clean_field(table_name, metric_name) %}
    
    {#- /* Apply to specific table */ -#}
    {%- if '_insights__' in table_name -%}
        {%- if metric_name == 'ad_id' -%}
        NULLIF(_sdc_source_key_{{metric_name}}::varchar,'') as {{metric_name}}

        {%- elif metric_name == 'date' -%}
        _sdc_source_key_{{metric_name}}::date as {{metric_name}}
        
        {%- endif -%}

    {%- elif 'ads_insights_placement__' in table_name -%}
        {%- if 'date' in metric_name -%}
        _sdc_source_key_{{metric_name}}::date as {{metric_name}}

        {%- else -%}
        NULLIF(_sdc_source_key_{{metric_name}}::varchar,'') as {{metric_name}}

        {%- endif -%}

    {%- elif table_name == 'adpreview' -%}
        {%- if metric_name == 'desktop_feed_standard' -%}
        {{metric_name}} as desktop_feed_preview_link
        
        {%- elif metric_name == 'mobile_feed_standard' -%}
        {{metric_name}} as mobile_feed_preview_link
        
        {%- elif metric_name == 'instagram_standard' -%}
        {{metric_name}} as instagram_preview_link
        
        {%- elif metric_name == 'instagram_story' -%}
        {{metric_name}} as instagram_story_preview_link

        {%- else -%}
        {{metric_name}}
        
        {%- endif -%}

    {%- elif table_name == 'ads__labels' -%}
        {%- if metric_name == 'id' -%}
        {{metric_name}} as ad_id
        
        {%- elif metric_name == 'name' -%}
        {{metric_name}} as label

        {%- else -%}
        {{metric_name}}
        
        {%- endif -%}
    
    {#- /*  End  */ -#}
    
    {# /* Apply to all tables */ #}
    {%- else -%}
    
        {%- if metric_name == 'id' -%}
        {{metric_name}}::bigint as {{table_name|trim('s')}}_{{metric_name}}

        {%- elif metric_name == 'creative__id' -%}
        NULLIF({{metric_name}}::varchar,'') as creative_id

        {%- elif metric_name == "account_id" -%}
        {{metric_name}}

        {%- elif '_time' in metric_name -%}
        {{metric_name}} as {{table_name|trim('s')}}_{{metric_name.split('_')[0]}}_at

        {%- else -%}
        {{metric_name}} as {{table_name|trim('s')}}_{{metric_name}}

        {%- endif -%}

    {%- endif -%}

{% endmacro -%}