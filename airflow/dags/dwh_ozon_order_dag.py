from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.external_task import ExternalTaskSensor

start_date = datetime(2020, 1, 1)
date_time_format='%Y-%m-%d %H:%M:%S'

dag = DAG(dag_id='wrk_dwh_ozon_order',
          start_date=start_date,
          schedule_interval='@monthly',
          description='dwh ozon orders',
          catchup=True)

last_dag_run = dag.get_last_dagrun()
last_date = start_date
if last_dag_run is not None:
    last_date = last_dag_run.execution_date

last_date = last_date.strftime(date_time_format)

stg = Variable.get("CLICK_STG_NAME")
dwh = Variable.get("CLICK_DWH_NAME")

wait_stg_ozon_task = ExternalTaskSensor(
    task_id='wait_stg_ozon',
    external_dag_id="wrk_ozon_to_stg",
    external_task_ids=["upload_orders_positions_to_click", "upload_orders_return_to_click"],
    check_existence=True,
    dag=dag
)

dwh_ozon_order_task = ClickHouseOperator(
    task_id='dwh_ozon_order',
    clickhouse_conn_id='CLICK_CONN',
    sql=(f"""insert into {dwh}.order (id, orderNumber, postingNumber, orderDate, endDate, typeId, customerId, positionId, price, salePrice, marketPrice, count, updateDate) 
            SELECT halfMD5(orderNumber,postingNumber,goodId )as id 
            ,o.orderNumber,o.postingNumber,o.fromD as orderDate, o.toD as endDate,to.id as typeId, o.customerId,op.goodId as positionId,op.totalPrice as price, op.salePrice, op.marketPrice as marketPrice, op.count as count,now() as updateDate
            FROM {stg}.ozon_order o 
            JOIN {stg}.ozon_order_position op on o.id=op.orderId 
            left join {dwh}.type_order to on to.name=o.ozonStatus and to.src='OZON'
            WHERE o.ozonStatus NOT IN ('cancelled') and o.fromD >= toDateTime('{last_date}') and op.totalPrice>0
            UNION DISTINCT
            SELECT halfMD5(orderNumber,postingNumber,ore.goodId,max(ore.returnId) )as id 
            ,o.orderNumber,o.postingNumber,o.fromD as orderDate, o.toD as endDate,16 as typeId, o.customerId,ore.goodId as positionId,op.totalPrice as price, op.salePrice, ore.marketPrice as marketPrice, toInt32(count(ore.count)) as count,now() as updateDate
            FROM {stg}.ozon_order_return ore
            join {stg}.ozon_order o on o.id=ore.orderId
            JOIN {stg}.ozon_order_position op on o.id=op.orderId  and op.goodId=ore.goodId
            WHERE  o.fromD >= toDateTime('{last_date}') and op.totalPrice>0
            group by o.orderNumber,o.postingNumber,o.fromD as orderDate, o.toD as endDate, o.customerId,ore.goodId as positionId,op.totalPrice as price, op.salePrice, ore.marketPrice""",
            f"optimize table {dwh}.order final"
    ),
    dag=dag)


wait_stg_ozon_task >> dwh_ozon_order_task
