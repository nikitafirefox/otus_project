#!/usr/bin/env sh

CLICK_DB_STG="${CLICK_DB_STG:-stg}";
CLICK_DB_PROD="${CLICK_DB_PROD:-dwh}";
CLICK_DB_USER="${CLICK_DB_USER:-admin}";
CLICK_DB_PASSWORD="${CLICK_DB_PASSWORD:-admin}";

cat <<EOT >> /etc/clickhouse-server/users.d/user.xml
<yandex>
  <!-- Docs: <https://clickhouse.tech/docs/en/operations/settings/settings_users/> -->
  <users>
    <${CLICK_DB_USER}>
      <profile>default</profile>
      <networks>
        <ip>::/0</ip>
      </networks>
      <password>${CLICK_DB_PASSWORD}</password>
      <quota>default</quota>
    </${CLICK_DB_USER}>
  </users>
</yandex>
EOT
#cat /etc/clickhouse-server/users.d/user.xml;

clickhouse-client --query "CREATE DATABASE IF NOT EXISTS ${CLICK_DB_STG}";
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS ${CLICK_DB_PROD}";

#WB
clickhouse-client --query "CREATE TABLE IF NOT EXISTS \
  ${CLICK_DB_STG}.wb_order ( \
  id Int32 NOT NULL, \
  orderNumber String NOT NULL, \
  fromD datetime NOT NULL, \
  toD datetime , \
  totalPrice Float32 NOT NULL, \
  wbStatus String, \
  wbOrderCustomerStatus String , \
  customerId Int32, \
  postingNumber String NOT NULL, \
  updateDate datetime NOT NULL \
)  ENGINE = ReplacingMergeTree(updateDate) \
 ORDER BY id \
 PRIMARY KEY (id)"; 

clickhouse-client --query "CREATE TABLE IF NOT EXISTS \
  ${CLICK_DB_STG}.wb_order_position ( \
  id Int32 NOT NULL, \
  goodId Int32, \
  totalPrice Float32, \
  salePrice Float32, \
  marketPrice Float32, \
  orderId Int32, \
  currencyCode String, \
  count Int32, \
  updateDate datetime NOT NULL \
) ENGINE = ReplacingMergeTree(updateDate) \
  ORDER BY id \
  PRIMARY KEY (id)";

##OZON
clickhouse-client --query "CREATE TABLE IF NOT EXISTS \
  ${CLICK_DB_STG}.ozon_order ( \
  id Int32 NOT NULL, \
  orderNumber String NOT NULL, \
  fromD datetime NOT NULL, \
  toD datetime, \
  totalPrice Float32 NOT NULL, \
  ozonStatus String, \
  customerId Int32 , \
  postingNumber String NOT NULL, \
  updateDate datetime NOT NULL \
)  ENGINE = ReplacingMergeTree(updateDate) \
 ORDER BY id \
 PRIMARY KEY (id)";

clickhouse-client --query "CREATE TABLE IF NOT EXISTS \
  ${CLICK_DB_STG}.ozon_order_position ( \
  id Int32 NOT NULL, \
  goodId Int32 , \
  totalPrice Float32 , \
  salePrice Float32, \
  marketPrice Float32 , \
  orderId Int32 , \
  currencyCode String, \
  count Int32, \
  updateDate datetime NOT NULL \
) ENGINE = ReplacingMergeTree(updateDate) \
  ORDER BY id \
  PRIMARY KEY (id)";

clickhouse-client --query "CREATE TABLE IF NOT EXISTS \
  ${CLICK_DB_STG}.ozon_order_return ( \
  id Int32 NOT NULL, \
  goodId Int32 , \
  marketPrice Float32 , \
  count Int32, \
  orderId Int32 , \
  returnId String , \
  updateDate datetime NOT NULL \
) ENGINE = ReplacingMergeTree(updateDate) \
  ORDER BY id \
  PRIMARY KEY (id)";

####1C
clickhouse-client --query "CREATE TABLE IF NOT EXISTS \
  ${CLICK_DB_STG}.1c_position ( \
  id Int32 NOT NULL, \
  goodId Int32 , \
  count Int32 NOT NULL, \
  totalPrice Float32 , \
  lastDate datetime NOT NULL, \
  updateDate datetime NOT NULL \
) ENGINE = ReplacingMergeTree(updateDate) \
  ORDER BY id \
  PRIMARY KEY (id)";

 clickhouse-client --query "CREATE TABLE IF NOT EXISTS \
  ${CLICK_DB_STG}.1c_marking ( \
  id Int32 NOT NULL, \
  weight Float32 , \
  typeName String NOT NULL, \
  typeCode String NOT NULL, \
  country String DEFAULT '-1', \
  season String DEFAULT '-1', \
  color String DEFAULT '-1', \
  createDate datetime, \
  lastDate datetime NOT NULL, \
  updateDate datetime NOT NULL \
) ENGINE = ReplacingMergeTree(updateDate) \
  ORDER BY id \
  PRIMARY KEY (id)"; 


###DWH
clickhouse-client --query "CREATE TABLE IF NOT EXISTS \
  ${CLICK_DB_PROD}.order ( \
  id UInt64 NOT NULL, \
  orderNumber String NOT NULL, \
  postingNumber String NOT NULL, \
  orderDate datetime NOT NULL, \
  endDate datetime, \
  typeId Int32 DEFAULT -1, \
  customerId Int32 DEFAULT -1, \
  positionId Int32 DEFAULT -1, \
  price Float32 NOT NULL, \
  salePrice Float32 NOT NULL, \
  marketPrice Float32 NOT NULL, \
  count Int32 NOT NULL, \
  updateDate datetime NOT NULL \
) ENGINE = ReplacingMergeTree(updateDate) \
  ORDER BY id \
  PRIMARY KEY (id)";

clickhouse-client --query "CREATE TABLE IF NOT EXISTS \
  ${CLICK_DB_PROD}.type_order ( \
  id Int32 NOT NULL, \
  name String NOT NULL, \
  status String, \
  src String \
) ENGINE = ReplacingMergeTree() \
  ORDER BY id \
  PRIMARY KEY (id)";

clickhouse-client --query "CREATE TABLE IF NOT EXISTS \
  ${CLICK_DB_PROD}.position ( \
  id Int32 NOT NULL, \
  goodId Int32 , \
  count Int32 NOT NULL, \
  totalPrice Float32 , \
  lastDate datetime NOT NULL, \
  updateDate datetime NOT NULL \
) ENGINE = ReplacingMergeTree(updateDate) \
  ORDER BY id \
  PRIMARY KEY (id)";

 clickhouse-client --query "CREATE TABLE IF NOT EXISTS \
  ${CLICK_DB_PROD}.marking ( \
  id Int32 NOT NULL, \
  weight Float32 , \
  typeName String NOT NULL, \
  typeCode String NOT NULL, \
  country String DEFAULT '-1', \
  season String DEFAULT '-1', \
  color String DEFAULT '-1', \
  createDate datetime, \
  lastDate datetime NOT NULL, \
  updateDate datetime NOT NULL \
  ) ENGINE = ReplacingMergeTree(updateDate) \
  ORDER BY id \
  PRIMARY KEY (id)"; 

clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select -1 as id,'Неопределено' as name,'Неопределено' as status,'DEFAULD' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=-1))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 1 as id,'complete' as name,'delivered' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=1))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 2 as id,'complete' as name,'sold' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=2))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 3 as id,'complete' as name,'cancelled' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=3))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 4 as id,'cancel' as name,'canceled' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=4))";

clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 5 as id,'complete' as name,'returning' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=5))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 6 as id,'in_process' as name,'cancelled' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=6))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 7 as id,'complete' as name,'canceled_by_client' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=7))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 8 as id,'complete' as name,'sorted' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=8))";

clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 9 as id,'new' as name,'canceled_by_client' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=9))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 10 as id,'complete' as name,'canceled' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=10))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 11 as id,'complete' as name,'defect' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=11))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 12 as id,'complete' as name,'ready_for_pickup' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=12))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 13 as id,'new' as name,'declined_by_client' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=13))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 14 as id,'complete' as name,'waiting' as status,'WB' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=14))";

clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 15 as id,'delivered' as name,'delivered' as status,'OZON' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=15))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 16 as id,'cancelled' as name,'cancelled' as status,'OZON' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=16))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 17 as id,'delivering' as name,'delivering' as status,'OZON' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=17))";
clickhouse-client --query "insert into ${CLICK_DB_PROD}.type_order (id,name,status,src) select 18 as id,'awaiting_packaging' as name,'awaiting_packaging' as status,'OZON' as src where 0=(select exists(select * from ${CLICK_DB_PROD}.type_order where id=18))";