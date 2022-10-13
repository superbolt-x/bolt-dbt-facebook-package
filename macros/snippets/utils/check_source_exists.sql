{% macro check_source_exists(schema_name, table_name) -%}

{%- set source_relation = adapter.get_relation(
      database=source(schema_name, table_name).database,
      schema=source(schema_name, table_name).schema,
      identifier=source(schema_name, table_name).name) -%}

{{ return(source_relation is not none) }}

{%- endmacro -%}