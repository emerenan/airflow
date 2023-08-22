{% set table_list = tables %}

{% for table in table_list %}
TRUNCATE TABLE {{ project_id }}.{{ dataset_id }}.{{table}};
{% endfor%}