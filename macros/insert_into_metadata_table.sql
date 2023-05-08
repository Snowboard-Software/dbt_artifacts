{% macro insert_into_metadata_table(database_name, schema_name, table_name, content) -%}
    {% if content != "" %}
        {{ return(adapter.dispatch('insert_into_metadata_table', 'dbt_artifacts')(database_name, schema_name, table_name, content)) }}
    {% endif %}
{%- endmacro %}

{% macro spark__insert_into_metadata_table(database_name, schema_name, table_name, content) -%}
    {% set insert_into_table_query %}
    insert into {% if database_name %}{{ database_name }}.{% endif %}{{ schema_name }}.{{ table_name }}
    {{ content }}
    {% endset %}

    {% do run_query(insert_into_table_query) %}
{%- endmacro %}

-- {% macro snowflake__insert_into_metadata_table(database_name, schema_name, table_name, content) -%}
--     {% set insert_into_table_query %}
--     insert into {{database_name}}.{{ schema_name }}.{{ table_name }}
--     {{ content }}
--     {% endset %}

--     {% do run_query(insert_into_table_query) %}
-- {%- endmacro %}

{% macro snowflake__insert_into_metadata_table(database_name, schema_name, table_name, content) -%}
{% set table_columns_query %}
select column_name, data_type
from information_schema.columns
where table_catalog = '{{database_name}}'
and table_schema = '{{schema_name}}'
and table_name = '{{table_name}}'
{% endset %}
{% set table_columns = run_query(table_columns_query) %}

{% set content_columns = content.split('\n')[0].split(',') %}
{% set content_columns = [column.strip() for column in content_columns] %}

{% set content_data_types = content.split('\n')[1].split(',') %}
{% set content_data_types = [data_type.strip() for data_type in content_data_types] %}

{% if len(table_columns) == len(content_columns) %}
    {% set column_check = True %}
    {% for index, column in enumerate(table_columns) %}
        {% if column.column_name != content_columns[index] or column.data_type != content_data_types[index] %}
            {% set column_check = False %}
            {% break %}
        {% endif %}
    {% endfor %}
    
    {% if column_check %}
        {% set insert_into_table_query %}
            insert into {{database_name}}.{{ schema_name }}.{{ table_name }}
            {{ content }}
        {% endset %}
        {% do run_query(insert_into_table_query) %}
    {% else %}
        {% set error_message = "The column names and/or data types in the content passed to the macro do not match the schema of the existing table." %}
        {% do log(error_message) %}
        {% do raise(error_message) %}
    {% endif %}
{% else %}
    {% set error_message = "The number of columns in the content passed to the macro does not match the schema of the existing table." %}
    {% do log(error_message) %}
    {% do raise(error_message) %}
{% endif %}
{%- endmacro %}


{% macro bigquery__insert_into_metadata_table(database_name, schema_name, table_name, content) -%}

        {% set insert_into_table_query %}
        insert into `{{database_name}}.{{ schema_name }}.{{ table_name }}`
        VALUES
        {{ content }}
        {% endset %}

        {% do run_query(insert_into_table_query) %}

{%- endmacro %}

{% macro default__insert_into_metadata_table(database_name, schema_name, table_name, content) -%}
{%- endmacro %}
