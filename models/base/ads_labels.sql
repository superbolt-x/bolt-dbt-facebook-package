{%- set selected_fields = [
    "id",
    "name"
] -%}
{%- set schema_name, table_name = 'facebook_raw', 'ads__labels' -%}


SELECT
 
    {% for field in selected_fields -%}
    {{ get_clean_field(table_name, field) }}
    {%- if not loop.last %},{%- endif %}
    {% endfor %}

FROM {{ source(schema_name, table_name) }}