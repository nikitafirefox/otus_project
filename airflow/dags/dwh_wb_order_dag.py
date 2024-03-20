from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

start_date = datetime(2020, 1, 1)
date_time_format='%Y-%m-%d %H:%M:%S'

dag = DAG(dag_id='wrk_dwh_wb_order',
          start_date=start_date,
          schedule_interval='@monthly',
          description='dwh wb orders',
          catchup=True)

last_dag_run = dag.get_last_dagrun()
last_date = start_date
if last_dag_run is not None:
    last_date = last_dag_run.execution_date

last_date = last_date.strftime(date_time_format)

stg = Variable.get("CLICK_STG_NAME")
dwh = Variable.get("CLICK_DWH_NAME")

wait_stg_wb_task = ExternalTaskSensor(
    task_id='wait_stg_wb',
    external_dag_id="wrk_wb_to_stg",
    external_task_id="upload_orders_positions_to_click",
    check_existence=True,
    dag=dag
)

dwh_wb_order_task = ClickHouseOperator(
    task_id='dwh_wb_order',
    clickhouse_conn_id='CLICK_CONN',
    sql=(f"""insert into {dwh}.order (id, orderNumber, postingNumber, orderDate, endDate, typeId, customerId, positionId, price, salePrice, marketPrice, count, updateDate) 
            SELECT halfMD5(orderNumber,postingNumber,goodId )as id 
            ,o.orderNumber,o.postingNumber,o.fromD as orderDate, o.toD as endDate,to.id as typeId, o.customerId,op.goodId as positionId,op.totalPrice as price, op.salePrice, op.marketPrice, op.count,now() as updateDate
            FROM {stg}.wb_order o 
            JOIN {stg}.wb_order_position op on o.id=op.orderId 
            join {dwh}.type_order to on to.name=o.wbStatus and to.status=o.wbOrderCustomerStatus 
            WHERE o.wbStatus NOT IN ('cancel') and o.fromD >= toDateTime('{last_date}') and op.totalPrice>0
            UNION DISTINCT
            SELECT halfMD5(orderNumber,postingNumber,goodId )as id
            ,o.orderNumber,o.postingNumber,o.fromD as orderDate, o.toD as endDate,to.id as typeId, o.customerId,op.goodId as positionId,op.totalPrice as price, op.salePrice, op.marketPrice, op.count,now() as updateDate
            FROM {stg}.wb_order o 
            JOIN {stg}.wb_order_position op on o.id=op.orderId 
            join {dwh}.type_order to on to.name=o.wbStatus and to.status=o.wbOrderCustomerStatus
            WHERE wbStatus IN ('cancel') and o.fromD >= toDateTime('{last_date}') and op.totalPrice>0""",
            f"optimize table {dwh}.order final"
    ),
    dag=dag)



wait_stg_wb_task >> dwh_wb_order_task
