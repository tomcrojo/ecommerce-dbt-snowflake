{% macro cents_to_euros(column_name, decimal_places=2) %}
    round({{ column_name }} / 100.0, {{ decimal_places }})
{% endmacro %}
