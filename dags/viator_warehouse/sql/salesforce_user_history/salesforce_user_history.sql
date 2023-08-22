
{% set label_list = labels %}
{% set columns = table_schema %}

{% macro get_ddl(project, dataset, table_name, materialisation, table_exists, partition_field, table_description) %} 
{% set table_or_view = 'TABLE' if materialisation in ('table', 'partition_table') else 'VIEW' %}
{% set command = ({'replace': 'OR REPLACE ','exists': ' IF NOT EXISTS' })%}
{% set ddl_command = table_or_view ~ command['exists'] if table_exists else command['replace'] ~ table_or_view %}
CREATE {{ ddl_command }} {{ project }}.{{ dataset }}.{{ table_name }}  {% if table_or_view != 'VIEW' %}
({% for col, col_type in columns.items() %}
    {{col}} {{col_type}}{% if not loop.last %},{% endif %}{% endfor %}
){% endif %} {% if materialisation == 'partition_table' %}
PARTITION BY {{ partition_field }} {% endif %}
OPTIONS(
    description="{{ table_description }}",
    labels=[{% for label in label_list %}{{label}}{% if not loop.last %}, {% endif %}{% endfor %}]
) {% endmacro %}

/*
Create the table based on the sql below
*/
{{ get_ddl(project_id, dataset_id, table_id, materialisation, table_exists, partition_field, table_description) }} AS
SELECT * FROM `avr-analysts.ccarnevalini.SalesforceUserHistory`;

/*
Starting Merge Statement with the created table in the before step
*/
Merge {{ project_id }}.{{ dataset_id }}.{{ table_id }} as A
using `avr-warehouse.ATTRSalesForce.User` as B
on A.id = B.id and A.RoleName = B.Role_Name__c
WHEN NOT MATCHED BY Target THEN
INSERT ( 
    Id, 
    Username, 
    Email, 
    Firstname, 
    Lastname, 
    Name, 
    RoleName, 
    StartDate, 
    EndDate )
VALUES (
    B.id, 
    B.username, 
    B.email, 
    B.firstname, 
    B.lastname, 
    B.name, 
    B.Role_Name__c, 
    current_date()-1, 
    '2999-12-31' );
/*
Create the a view based on table create
*/
{{ get_ddl(vw_project_id, vw_dataset_id, vw_table_id, vw_materialisation, vw_table_exists, '', table_description) }} AS
with cte as (
select * 
    ,ROW_NUMBER() OVER (PARTITION BY Id ORDER BY StartDate ASC) as RowNum
from {{ project_id }}.{{ dataset_id }}.{{ table_id }} 
)
,base as (
select 
    a.Id
    , a.Username
    , a.Email
    , a.Firstname
    , a.Lastname
    , a.Name
    , a.RoleName
    , a.StartDate
    , ifnull(b.StartDate,'2999-12-31') as EndDate
from cte a
left join cte b
    ON a.id=B.id AND a.RowNum=B.RowNum-1
)
select * from base;