CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL} ( 
    Actual_Delivery_Date string,
    CustomerKey string,
    DateKey string,
    Discount_Amount string,
    Invoice_Date string,
    Invoice_Number string,
    Item_Class string,
    Item_Number string,
    Item string,
    Line_Number string,
    List_Price string,
    Order_Number string,
    Promised_Delivery_Date string,
    Sales_Amount string,
    Sales_Amount_Based_List_Price string,
    Sales_Cost_Amount string,
    Sales_Margin_Amount string,
    Sales_Price string,
    Sales_Quantity string,
    Sales_Rep string,
    U_M string
    )
COMMENT 'Tabela de CLIENTES'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
location '${HDFS_DIR}'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA} (
    Actual_Delivery_Date string,
    CustomerKey string,
    DateKey string,
    Discount_Amount string,
    Invoice_Date string,
    Invoice_Number string,
    Item_Class string,
    Item_Number string,
    Item string,
    Line_Number string,
    List_Price string,
    Order_Number string,
    Promised_Delivery_Date string,
    Sales_Amount string,
    Sales_Amount_Based_List_Price string,
    Sales_Cost_Amount string,
    Sales_Margin_Amount string,
    Sales_Price string,
    Sales_Quantity string,
    Sales_Rep string,
    U_M string
)
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
TBLPROPERTIES ( 'orc.compress'='SNAPPY');


SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    ${TARGET_DATABASE}.${TARGET_TABLE_GERENCIADA}
PARTITION(DT_FOTO) 
SELECT
    Actual_Delivery_Date string,
    CustomerKey string,
    DateKey string,
    Discount_Amount string,
    Invoice_Date string,
    Invoice_Number string,
    Item_Class string,
    Item_Number string,
    Item string,
    Line_Number string,
    List_Price string,
    Order_Number string,
    Promised_Delivery_Date string,
    Sales_Amount string,
    Sales_Amount_Based_List_Price string,
    Sales_Cost_Amount string,
    Sales_Margin_Amount string,
    Sales_Price string,
    Sales_Quantity string,
    Sales_Rep string,
    U_M string,
	${PARTICAO} as DT_FOTO
FROM ${TARGET_DATABASE}.${TARGET_TABLE_EXTERNAL}