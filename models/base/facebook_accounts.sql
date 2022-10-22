{%- set selected_fields = [
    "id",
    "name",
    "currency",
    "_fivetran_synced"
] -%}
{%- set schema_name, table_name = 'facebook_raw', 'accounts' -%}

WITH staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_facebook_clean_field(table_name, field) }},
        {% endfor -%}
        MAX(_fivetran_synced) OVER (PARTITION BY id) as last_updated_time

    FROM {{ source(schema_name, table_name) }}
    )

SELECT *
FROM staging 
WHERE _fivetran_synced = last_updated_time