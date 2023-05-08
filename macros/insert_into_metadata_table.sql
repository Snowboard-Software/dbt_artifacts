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

{% macro snowflake__insert_into_metadata_table(database_name, schema_name, table_name, content) -%}
    {% set table_columns_query %}
        select column_name, data_type
        from information_schema.columns
        where table_catalog = '{{database_name}}'
        and table_schema = '{{schema_name}}'
        and table_name = '{{table_name}}'
    {% endset %}

    {% do log(content) %}
    
    {% set table_columns = run_query(table_columns_query) %}

    {% do log(table_columns) %}
    
    {% set content_columns = content.split('\n')[0].split(',') %}
    {% set content_columns_list = [] %}
    {% for column in content_columns %}
        {% set column_trimmed = column.strip() %}
        {% do content_columns_list.append(column_trimmed) %}
    {% endfor %}

    {% do log(content_columns_list) %}
    
    {% set content_data_types = content.split('\n')[1].split(',') %}
    {% set content_data_types_list = [] %}
    {% for data_type in content_data_types %}
        {% set data_type_trimmed = data_type.strip() %}
        {% do content_data_types_list.append(data_type_trimmed) %}
    {% endfor %}

    {% do log(content_data_types_list) %}

    {% do log(len(table_columns)) %}
    {% do log(len(content_columns_list)) %}
    
    {% if len(table_columns) == len(content_columns_list) %}
        {% set column_check = True %}
        {% for index, column in enumerate(table_columns) %}
            {% if column.column_name != content_columns_list[index] or column.data_type != content_data_types_list[index] %}
                {% set column_check = False %}
            {% endif %}
        {% else %}
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
        {% endfor %}
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
