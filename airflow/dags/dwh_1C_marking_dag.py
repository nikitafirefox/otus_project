from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

start_date = datetime(2020, 1, 1)
date_time_format='%Y-%m-%d %H:%M:%S'

dag = DAG(dag_id='wrk_dwh_1C_marking',
          start_date=start_date,
          schedule_interval='@monthly',
          description='dwh 1C marking',
          catchup=True)

last_dag_run = dag.get_last_dagrun()
last_date = start_date
if last_dag_run is not None:
    last_date = last_dag_run.execution_date

last_date = last_date.strftime(date_time_format)

stg = Variable.get("CLICK_STG_NAME")
dwh = Variable.get("CLICK_DWH_NAME")

wait_stg_1c_task = ExternalTaskSensor(
    task_id='wait_stg_1c',
    external_dag_id="wrk_1c_to_stg",
    external_task_ids=["upload_positions_to_click", "upload_markings_to_click"],
    check_existence=True,
    dag=dag
)

dwh_1C_marking_task = ClickHouseOperator(
    task_id='dwh_1C_marking',
    clickhouse_conn_id='CLICK_CONN',
    sql=(f"""insert into {dwh}.marking (id, weight, typeName, typeCode, country, season, color, createDate, lastDate, updateDate) 
            SELECT o.id as id 
            ,weight, typeName, typeCode, country, season, color, createDate, lastDate, now() as updateDate
            FROM {stg}.1c_marking o 
            WHERE o.lastDate >= toDateTime('{last_date}')""",
            f"optimize table {dwh}.marking final"
    ),
    dag=dag)

dwh_1c_position_task = ClickHouseOperator(
    task_id='dwh_1c_position',
    clickhouse_conn_id='CLICK_CONN',
    sql=(f"""insert into {dwh}.position (id, goodId, count, totalPrice, lastDate, updateDate) 
            SELECT o.id as id 
            , goodId, count, totalPrice, lastDate, now() as updateDate
            FROM {stg}.1c_position o  
            WHERE o.lastDate >= toDateTime('{last_date}')""",
            f"optimize table {dwh}.position final"
    ),
    dag=dag)

wait_stg_1c_task >> dwh_1C_marking_task >> dwh_1c_position_task
