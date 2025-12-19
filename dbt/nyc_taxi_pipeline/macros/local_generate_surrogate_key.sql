{% macro generate_surrogate_key(columns) %}
    md5(
        {% for col in columns %}
            coalesce(cast({{ col }} as varchar), '_dbt_utils_surrogate_key_null_')
            {% if not loop.last %} || '-' || {% endif %}
        {% endfor %}
    )
{% endmacro %}
