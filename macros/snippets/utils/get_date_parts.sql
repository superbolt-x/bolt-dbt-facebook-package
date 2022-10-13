{%- macro get_date_parts(date_column) -%}

{%- for date_part in ['week', 'month','quarter','year'] %}
    {%- if date_part == 'week' -%}
    {%- if var('week_start')|lower in ('mon','monday') %}
    DATE_TRUNC('{{date_part}}', {{date_column}})::date as {{date_part}}
    {%- elif var('week_start')|lower in ('tue','tues','tuesday') %}
    (DATE_TRUNC('{{date_part}}', {{date_column}}+6)-6)::date as {{date_part}}
    {%- elif var('week_start')|lower in ('wed','wednesday') %}
    (DATE_TRUNC('{{date_part}}', {{date_column}}+5)-5)::date as {{date_part}}
    {%- elif var('week_start')|lower in ('thu','thur','thurs','thursday') %}
    (DATE_TRUNC('{{date_part}}', {{date_column}}+4)-4)::date as {{date_part}}
    {%- elif var('week_start')|lower in ('fri','friday') %}
    (DATE_TRUNC('{{date_part}}', {{date_column}}+3)-3)::date as {{date_part}}
    {%- elif var('week_start')|lower in ('sat','saturday') %}
    (DATE_TRUNC('{{date_part}}', {{date_column}}+2)-2)::date as {{date_part}}
    {%- elif var('week_start')|lower in ('sun','sunday') %}
    (DATE_TRUNC('{{date_part}}', {{date_column}}+1)-1)::date as {{date_part}}
    {%- endif -%}
    {%- else %}
    DATE_TRUNC('{{date_part}}', {{date_column}})::date as {{date_part}}
    {%- endif -%}
{%- if not loop.last %},{%- endif -%}
{% endfor -%}   
  
{%- endmacro -%}