TABLES=("CLIENTES" "DIVISAO" "ENDERECO" "REGIAO" "VENDAS")

for table in "${TABLES[@]}"
do
    TARGET_DATABASE="DESAFIO_CURSO"
    HDFS_DIR="/datalake/raw/$table"
    TARGET_TABLE_EXTERNAL="$table"
    TARGET_TABLE_GERENCIADA="tbl_$table"
    PARTICAO="$(date --date="-0 day" "+%Y%m%d")"

    beeline -u jdbc:hive2://localhost:10000 \
    --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
    --hivevar HDFS_DIR="${HDFS_DIR}"\
    --hivevar TARGET_TABLE_EXTERNAL="${TARGET_TABLE_EXTERNAL}"\
    --hivevar TARGET_TABLE_GERENCIADA="${TARGET_TABLE_GERENCIADA}"\
    --hivevar PARTICAO="${PARTICAO}"\
    -f ../hql/create_table_$table.hql 
done