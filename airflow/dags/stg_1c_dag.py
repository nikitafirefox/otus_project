import requests
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

start_date = datetime(2020, 1, 1)
date_time_format='%Y-%m-%d %H:%M:%S'

dag = DAG(dag_id='wrk_1c_to_stg',
          start_date=start_date,
          schedule_interval='@monthly',
          description='1c positions transfer',
          catchup=True)

endpoint = Variable.get("API_1C_ENDPOINT")

last_dag_run = dag.get_last_dagrun()
last_date = start_date
if last_dag_run is not None:
    last_date = last_dag_run.execution_date

# POSITIONS

def get_positions(**kwargs):
    res = requests.get(f"{endpoint}/positions?date={last_date}")
    return res.json()

get_positions_task = PythonOperator(
    task_id='get_positions', 
    python_callable=get_positions,
    provide_context=True,
    dag=dag, 
)

def transform_positions(**kwargs):
    ti = kwargs['ti']
    arr = ti.xcom_pull(task_ids='get_positions')
    records = []
    if isinstance(arr, list):
        for o in arr:
            model = {
                'id': o['id'],
                'goodId': o['goodId'],
                'count': o['count'],
                'totalPrice': o['totalPrice'],
                'lastDate': datetime.strptime(o['lastDate'], date_time_format),
                'updateDate': datetime.now()
            }
            records.append(model)
    return records

transform_positions_task = PythonOperator(
    task_id='transform_positions', 
    python_callable=transform_positions,
    provide_context=True,
    dag=dag, 
)

def upload_positions_to_click(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform_positions')
    if len(records) > 0:
        ch_hook = ClickHouseHook(clickhouse_conn_id='CLICK_CONN', database=Variable.get("CLICK_STG_NAME"))
        ch_hook.execute('INSERT INTO 1c_position VALUES', records)
        ch_hook.execute('optimize table 1c_position final')

upload_positions_to_click_task = PythonOperator(
    task_id='upload_positions_to_click', 
    python_callable=upload_positions_to_click,
    provide_context=True,
    dag=dag)

# MARKINGS

def get_markings(**kwargs):
    res = requests.get(f"{endpoint}/markings?date={last_date}")
    return res.json()

get_markings_task = PythonOperator(
    task_id='get_markings', 
    python_callable=get_markings,
    provide_context=True,
    dag=dag, 
)

def transform_markings(**kwargs):
    ti = kwargs['ti']
    arr = ti.xcom_pull(task_ids='get_markings')
    records = []
    if isinstance(arr, list):
        for o in arr:
            model = {
                'id': o['id'],
                'weight': o['weight'],
                'typeName': o['typeName'],
                'typeCode': o['typeCode'],
                'country': o['country'] if o['country'] is not None else '-1',
                'season': o['season'] if o['season'] is not None else '-1',
                'color': o['color'] if o['color'] is not None else '-1',
                'createDate': datetime.strptime(o['createdDate'], date_time_format),
                'lastDate': datetime.strptime(o['updatedDate'], date_time_format),
                'updateDate': datetime.now()
            }
            records.append(model)
    return records

transform_markings_task = PythonOperator(
    task_id='transform_markings', 
    python_callable=transform_markings,
    provide_context=True,
    dag=dag, 
)

def upload_markings_to_click(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform_markings')
    if isinstance(records, list) and len(records) > 0:
        ch_hook = ClickHouseHook(clickhouse_conn_id='CLICK_CONN', database=Variable.get("CLICK_STG_NAME"))
        ch_hook.execute('INSERT INTO 1c_marking VALUES', records)
        ch_hook.execute('optimize table 1c_marking final')

upload_markings_to_click_task = PythonOperator(
    task_id='upload_markings_to_click', 
    python_callable=upload_markings_to_click,
    provide_context=True,
    dag=dag)

get_positions_task >> transform_positions_task >> upload_positions_to_click_task
get_markings_task >> transform_markings_task >> upload_markings_to_click_task
