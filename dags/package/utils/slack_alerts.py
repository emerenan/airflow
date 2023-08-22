from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from re import sub

def slack_agg_base(context, **kwargs):
    
    SLACK_CONN_ID = "slack_notifications"
    CHANNEL_NAME = '#vi_analytics_dag_alerts'
    SLACK_ENVIRONMENT = Variable.get("slack_environment")
    
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    
    failed_tasks = []
    ti = context['task_instance']
    for t in ti.get_dagrun().get_task_instances(state="failed"):  # type: TaskInstance
        failed_tasks.append({'id': t.task_id, 'url': t.log_url})
    
    task_info = ""
    for failed_task in failed_tasks:
        task_info += """
        Task: {task}, 
        URL: {url} \n""".format(task=sub("([a-zA-Z0-9_.+-]+)\.([a-zA-Z0-9_.+-]+)\.","",failed_task['id']), url=failed_task['url'])
    
    slack_msg = """
        {env}:{icon}: Airflow DAG Failure Report 
        *DAG*: {dag}
        *Execution Time*: {exec_date}
        *Tasks Details*: \n{tasks}
        """.format(
                env=SLACK_ENVIRONMENT,
                icon=kwargs['icon'],
                tasks=task_info,
                dag=context.get('task_instance').dag_id,
                exec_date=context.get('execution_date').__str__()
            )
                
    failed_alert = SlackWebhookOperator(
        task_id=context.get('task_instance').dag_id,
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=CHANNEL_NAME)
    return failed_alert.execute(context=context)

def slack_fail_alerts(context):
    return slack_agg_base(context, icon='rotating_light')
