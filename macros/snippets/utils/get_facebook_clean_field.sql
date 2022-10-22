{%- macro get_facebook_clean_field(table_name, column_name) %}
    
    {#- /* Apply to specific table */ -#}
    {%- if '_insights__' in table_name -%}
        {%- if column_name == 'ad_id' -%}
        NULLIF(_sdc_source_key_{{column_name}}::varchar,'') as {{column_name}}
        
        {%- endif -%}

    {%- elif 'ads_insights_placement__' in table_name -%}
        {%- if 'date' in column_name -%}
        _sdc_source_key_{{column_name}}::date as {{column_name}}

        {%- else -%}
        NULLIF(_sdc_source_key_{{column_name}}::varchar,'') as {{column_name}}

        {%- endif -%}

    {%- elif table_name == 'adpreview' -%}
        {%- if column_name == 'desktop_feed_standard' -%}
        {{column_name}} as desktop_feed_preview_link
        
        {%- elif column_name == 'mobile_feed_standard' -%}
        {{column_name}} as mobile_feed_preview_link
        
        {%- elif column_name == 'instagram_standard' -%}
        {{column_name}} as instagram_preview_link
        
        {%- elif column_name == 'instagram_story' -%}
        {{column_name}} as instagram_story_preview_link

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif table_name == 'ads__labels' -%}
        {%- if column_name == 'id' -%}
        {{column_name}} as ad_id
        
        {%- elif column_name == 'name' -%}
        {{column_name}} as label

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}
    
    {#- /*  End  */ -#}
    
    {# /* Apply to all tables */ #}
    {%- else -%}
    
        {%- if column_name == 'id' -%}
        {{column_name}}::bigint as {{table_name|trim('s')}}_{{column_name}}

        {%- elif column_name == 'creative__id' -%}
        NULLIF({{column_name}}::varchar,'') as creative_id

        {%- elif column_name in ("account_id","_fivetran_synced","updated_time") -%}
        {{column_name}}

        {%- else -%}
        {{column_name}} as {{table_name|trim('s')}}_{{column_name}}

        {%- endif -%}

    {%- endif -%}

{% endmacro -%}