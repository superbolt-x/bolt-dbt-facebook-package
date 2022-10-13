{%- set selected_fields = [
    "id",
    "name",
    "effective_status",
    "account_id",
    "updated_time"
] -%}
{%- set schema_name, table_name = 'facebook_raw', 'ads' -%}

WITH staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_clean_field(table_name, field) }},
        {% endfor -%}
        updated_time,
        MAX(updated_time) OVER (PARTITION BY id) as last_updated_time

    FROM {{ source(schema_name, table_name) }}
    )

SELECT *
FROM staging 
WHERE updated_time = last_updated_time