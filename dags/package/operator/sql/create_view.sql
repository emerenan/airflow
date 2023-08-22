DROP TABLE `{{source}}`;
                    
CREATE VIEW IF NOT EXISTS `{{source}}`
OPTIONS(
    description="{{description}}",
    labels=[("dag_id", "{{dag_name}}")]
    )
AS 
SELECT {% if columns == 'None' %}
*{% else %}
{{columns}}{% endif %}
{% if partition_field == 'True' %}
,_PARTITIONTIME as PARTITIONTIME
{% endif %}
FROM `{{destination}}`;