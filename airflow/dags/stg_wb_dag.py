from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import ShortCircuitOperator

start_date = datetime(2020, 1, 1)

dag = DAG(dag_id='wrk_wb_to_stg',
          start_date=start_date,
          schedule_interval='@monthly',
          description='wb orders transfer',
          catchup=True)

wb_db_name = Variable.get("WB_DB_NAME")

last_dag_run = dag.get_last_dagrun()
last_date = start_date
if last_dag_run is not None:
    last_date = last_dag_run.execution_date

# ORDERS

get_wb_orders_task = MySqlOperator(
    task_id='get_wb_orders',
    mysql_conn_id='WB_CONN',
    sql=f"select * from {wb_db_name}.order where fromD >= '{last_date}'",
    dag=dag)

def transform_orders(**kwargs):
    ti = kwargs['ti']
    rows = ti.xcom_pull(task_ids='get_wb_orders')
    records = []
    for row in rows:
        model = {
            'id': row[0],
            'orderNumber': row[1],
            'fromD': row[2],
            'toD': row[3],
            'totalPrice': row[4],
            'wbStatus': row[5],
            'wbOrderCustomerStatus': row[6],
            'customerId': row[7] if row[7] is not None else -1,
            'postingNumber': row[8],
            'updateDate': datetime.now()
        }
        records.append(model)
    return records

transform_orders_task = PythonOperator(
    task_id='transform_orders', 
    python_callable=transform_orders,
    provide_context=True,
    dag=dag, 
)

def isNotEmptyOrders(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform_orders')
    return True if len(records)> 0 else False

is_not_empty_orders_task = ShortCircuitOperator(
    task_id='is_not_empty',
    python_callable=isNotEmptyOrders,
    ignore_downstream_trigger_rules=False,
    dag=dag)


def upload_orders_to_click(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform_orders')
    ch_hook = ClickHouseHook(clickhouse_conn_id='CLICK_CONN', database=Variable.get("CLICK_STG_NAME"))
    ch_hook.execute('INSERT INTO wb_order VALUES', records)
    ch_hook.execute('optimize table wb_order final')

upload_orders_to_click_task = PythonOperator(
    task_id='upload_orders_to_click', 
    python_callable=upload_orders_to_click,
    provide_context=True,
    dag=dag)

# ORDERS POSITION

get_wb_orders_positions_task = MySqlOperator(
    task_id='get_wb_orders_posions',
    mysql_conn_id='WB_CONN',
    sql=f"select op.id,op.goodId,op.totalPrice,op.salePrice,op.marketPrice,op.orderId,op.currencyCode,op.count from {wb_db_name}.order_position op join {wb_db_name}.order o on o.id=op.orderId and o.fromD >= '{last_date}'",
    dag=dag)

def transform_orders_positions(**kwargs):
    ti = kwargs['ti']
    rows = ti.xcom_pull(task_ids='get_wb_orders_posions')
    records = []
    for row in rows:
        model = {
            'id': row[0],
            'goodId': row[1],
            'totalPrice': row[2],
            'salePrice': float(row[3]) if row[3] is not None else row[2],
            'marketPrice': row[4],
            'orderId': row[5],
            'currencyCode': row[6],
            'count': row[7],
            'updateDate': datetime.now()
        }
        records.append(model)
    return records

transform_orders_positions_task = PythonOperator(
    task_id='transform_orders_positions', 
    python_callable=transform_orders_positions,
    provide_context=True,
    dag=dag, 
)

def upload_orders_positions_to_click(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform_orders_positions')
    ch_hook = ClickHouseHook(clickhouse_conn_id='CLICK_CONN', database=Variable.get("CLICK_STG_NAME"))
    ch_hook.execute('INSERT INTO wb_order_position VALUES', records)
    ch_hook.execute('optimize table wb_order_position final')

upload_orders_positions_to_click_task = PythonOperator(
    task_id='upload_orders_positions_to_click', 
    python_callable=upload_orders_positions_to_click,
    provide_context=True,
    dag=dag)

get_wb_orders_task >> transform_orders_task >> is_not_empty_orders_task >> [upload_orders_to_click_task, get_wb_orders_positions_task]
get_wb_orders_positions_task >> transform_orders_positions_task >> upload_orders_positions_to_click_task