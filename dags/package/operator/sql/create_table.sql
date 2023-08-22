{% if is_backup == True %}
CREATE TABLE IF NOT EXISTS `{{destination}}_BCK_{{ref_date}}`{% else %}
CREATE TABLE IF NOT EXISTS `{{destination}}`{% endif %}
COPY `{{source}}`
OPTIONS (
    description="{{description}}",
    labels=[("dag_id", "{{dag_name}}"),("task_id","{{ task_name }}")]
);