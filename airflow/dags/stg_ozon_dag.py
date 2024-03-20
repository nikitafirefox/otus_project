from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.operators.python import ShortCircuitOperator

start_date = datetime(2020, 1, 1)

dag = DAG(dag_id='wrk_ozon_to_stg',
          start_date=start_date,
          schedule_interval='@monthly',
          description='ozon orders transfer',
          catchup=True)

ozon_db_name = Variable.get("OZON_DB_NAME")
ozon_db_schema = Variable.get("OZON_DB_SCHEMA")

last_dag_run = dag.get_last_dagrun()
last_date = start_date
if last_dag_run is not None:
    last_date = last_dag_run.execution_date

# ORDERS

get_ozon_orders_task = PostgresOperator(
    task_id='get_ozon_orders',
    postgres_conn_id='OZON_CONN',
    sql=f"select * from {ozon_db_name}.{ozon_db_schema}.order where fromd >= '{last_date}'",
    dag=dag)

def transform_orders(**kwargs):
    ti = kwargs['ti']
    rows = ti.xcom_pull(task_ids='get_ozon_orders')
    records = []
    for row in rows:
        model = {
            'id': row[0],
            'orderNumber': row[1],
            'fromD': row[2],
            'toD': row[3],
            'totalPrice': row[4],
            'ozonStatus': row[5],
            'customerId': row[6] if row[6] is not None else -1,
            'postingNumber': row[7],
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
    ch_hook.execute('INSERT INTO ozon_order VALUES', records)
    ch_hook.execute('optimize table ozon_order final')

upload_orders_to_click_task = PythonOperator(
    task_id='upload_orders_to_click', 
    python_callable=upload_orders_to_click,
    provide_context=True,
    dag=dag)

# ORDERS POSITION

get_ozon_orders_positions_task = PostgresOperator(
    task_id='get_ozon_orders_posions',
    postgres_conn_id='OZON_CONN',
    sql=f"select op.id,op.goodid,op.totalprice,op.saleprice,op.marketprice,op.orderid,op.currencycode,op.count from {ozon_db_name}.{ozon_db_schema}.order_position op join {ozon_db_name}.{ozon_db_schema}.order o on o.id=op.orderid and o.fromd >= '{last_date}'",
    dag=dag)

def transform_orders_positions(**kwargs):
    ti = kwargs['ti']
    rows = ti.xcom_pull(task_ids='get_ozon_orders_posions')
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
    ch_hook.execute('INSERT INTO ozon_order_position VALUES', records)
    ch_hook.execute('optimize table ozon_order_position final')

upload_orders_positions_to_click_task = PythonOperator(
    task_id='upload_orders_positions_to_click', 
    python_callable=upload_orders_positions_to_click,
    provide_context=True,
    dag=dag)

# ORDERS RETURN

get_ozon_orders_return_task = PostgresOperator(
    task_id='get_ozon_orders_return',
    postgres_conn_id='OZON_CONN',
    sql=f"select op.id,op.goodid,op.marketprice,op.orderid,op.count, op.returnid from {ozon_db_name}.{ozon_db_schema}.order_return op join {ozon_db_name}.{ozon_db_schema}.order o on o.id=op.orderid and o.fromd >= '{last_date}'",
    dag=dag)

def transform_orders_return(**kwargs):
    ti = kwargs['ti']
    rows = ti.xcom_pull(task_ids='get_ozon_orders_return')
    records = []
    for row in rows:
        model = {
            'id': row[0],
            'goodId': row[1],
            'marketPrice': row[2],
            'orderId': row[3],
            'count': row[4],
            'returnId': row[5],
            'updateDate': datetime.now()
        }
        records.append(model)
    return records

transform_orders_return_task = PythonOperator(
    task_id='transform_orders_return', 
    python_callable=transform_orders_return,
    provide_context=True,
    dag=dag, 
)

def upload_orders_return_to_click(**kwargs):
    ti = kwargs['ti']
    records = ti.xcom_pull(task_ids='transform_orders_return')
    ch_hook = ClickHouseHook(clickhouse_conn_id='CLICK_CONN', database=Variable.get("CLICK_STG_NAME"))
    ch_hook.execute('INSERT INTO ozon_order_return VALUES', records)
    ch_hook.execute('optimize table ozon_order_return final')

upload_orders_return_to_click_task = PythonOperator(
    task_id='upload_orders_return_to_click', 
    python_callable=upload_orders_return_to_click,
    provide_context=True,
    dag=dag)   

get_ozon_orders_task >> transform_orders_task >> is_not_empty_orders_task >> [upload_orders_to_click_task, get_ozon_orders_positions_task, get_ozon_orders_return_task]
get_ozon_orders_positions_task >> transform_orders_positions_task >> upload_orders_positions_to_click_task
get_ozon_orders_return_task >> transform_orders_return_task >> upload_orders_return_to_click_task