{%- set selected_fields = [
    "id",
    "name",
    "billing_event",
    "bid_strategy",
    "daily_budget",
    "effective_status",
    "optimization_goal",
    "account_id",
    "updated_time"
] -%}
{%- set schema_name, table_name = 'facebook_raw', 'adsets' -%}

WITH staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_facebook_clean_field(table_name, field) }},
        {% endfor -%}
        updated_time,
        MAX(updated_time) OVER (PARTITION BY id) as last_updated_time

    FROM {{ source(schema_name, table_name) }}
    )

SELECT *
FROM staging 
WHERE updated_time = last_updated_time