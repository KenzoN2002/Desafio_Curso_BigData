from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import *
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.functions import col as spark_col
import os
import re

spark = SparkSession.builder.master("local[*]")\    
        .enableHiveSupport()\    
        .getOrCreate()


# Criação dos dataframes
df_clientes = spark.sql("SELECT * FROM DESAFIO_CURSO.TBL_CLIENTES")
df_divisao = spark.sql("Select * from DESAFIO_CURSO.TBL_DIVISAO")
df_endereco = spark.sql("Select * from DESAFIO_CURSO.TBL_ENDERECO")
df_regiao = spark.sql("Select * from DESAFIO_CURSO.TBL_REGIAO")
df_vendas = spark.sql("Select * from DESAFIO_CURSO.TBL_VENDAS")

# Mostrar os dataframes
df_clientes.show(truncate=False, n=10)
df_divisao.show(truncate=False, n=10)
df_endereco.show(truncate=False, n=10)
df_regiao.show(truncate=False, n=10)
df_vendas.show(truncate=False, n=10)

df_clientes_string = ["business_family","customer", "customer_type", "line_business","phone","regional_sales_mgr","search_type"]
df_clientes_num = ["address_number", "business_unit", "customer_key", "division", "region_code"]
df_endereco_string = ["city", "country", "customer_address_1","customer_address_2","customer_address_3","customer_address_4", "state"]
df_endereco_num = ["address_number", "zip_code"]
df_vendas_string = ["item_class","item","u_m"]
df_vendas_num = ["actual_delivery_date","customerkey","discount_amount","invoice_number","item_number","line_number","list_price","order_number","sales_amount","sales_amount_based_list_price","sales_cost_amount","sales_margin_amount","sales_price","sales_quantity","sales_rep"]

# tratamento dos campos vazios do df-clientes
for column_name in df_clientes_string: 
    df_clientes = df_clientes.withColumn(column_name,when(regexp_replace(spark_col(column_name)," ","") == "", "Não Informado").otherwise(spark_col(column_name)))
for col in df_clientes_num: 
    df_clientes = df_clientes.withColumn(column_name,when(regexp_replace(spark_col(column_name)," ","") == "", "0").otherwise(spark_col(column_name)))
df_clientes.show(truncate=False)

# tratamento dos campos vazios do df-endereco
for column_name in df_endereco_string: 
    df_endereco = df_endereco.withColumn(column_name, when(regexp_replace(spark_col(column_name)," ","") == "", "Não Informado").otherwise(spark_col(column_name)))
for col in df_endereco_num: 
    df_endereco = df_endereco.withColumn(column_name, when(regexp_replace(spark_col(column_name)," ","") == "", "0").otherwise(spark_col(column_name)))
df_endereco.show(truncate=False)

# tratamento dos campos vazios do df-vendas
for column_name in df_vendas_string: 
    df_vendas = df_vendas.withColumn(column_name, when(regexp_replace(spark_col(column_name)," ","") == "", "Não Informado").otherwise(spark_col(column_name)))
for column_name in df_vendas_num: 
    df_vendas = df_vendas.withColumn(column_name, when(regexp_replace(spark_col(column_name)," ","") == "", "0").otherwise(spark_col(column_name)))
df_vendas.show(truncate=False)

df_clientes = df_clientes.dropDuplicates()
df_endereco = df_endereco.dropDuplicates()

# MOSTRA CONTAGEM DOS DFs E FAZ OS JOINS

print("===============================================================")
print('CONTAGEM DAS LINHAS DO DATAFRAME DE CLIENTES: ', df_clientes.count())
print("===============================================================")
print("===============================================================")
print('CONTAGEM DAS LINHAS DO DATAFRAME DE ENDERECO: ', df_endereco.count())
print("===============================================================")
print("===============================================================")
print('CONTAGEM DAS LINHAS DO DATAFRAME DE VENDAS: ', df_vendas.count())
print("===============================================================")
df_clientes = df_clientes.select(spark_col("address_number"), spark_col("customer"), spark_col("customerkey"), spark_col("customer_type"), spark_col("division"), spark_col("region_code"))
df_endereco = df_endereco.select(spark_col("address_number"), spark_col("country"),spark_col("state"), spark_col("city"))
df_vendas = df_vendas.select(spark_col("customerkey"),spark_col("invoice_date"),spark_col("item_class"),spark_col("item"),spark_col("sales_amount"),spark_col("sales_price"),spark_col("sales_quantity"))
df_regiao = df_regiao.select(spark_col("region_code"), spark_col("region_name"))
df_divisao = df_divisao.select(spark_col("division"), spark_col("division_name"))

df_stage = df_vendas.join(df_clientes, df_vendas.customerkey == df_clientes.customerkey, how="inner").drop(df_clientes.customerkey)
df_stage = df_stage.join(df_endereco, df_stage.address_number == df_endereco.address_number, how="left").drop(df_endereco.address_number)


df_stage = df_stage.join(df_regiao, df_stage.region_code == df_regiao.region_code, how="left").drop(df_regiao.region_code)
df_stage = df_stage.join(df_divisao, df_stage.division == df_divisao.division, how="left").drop(df_divisao.division)
df_stage.show(truncate=False)


# Criação dos Campos Calendario
df_stage = df_stage.withColumn("invoice_date", regexp_replace(spark_col("invoice_date"),"/","-"))\                   
                    .withColumn("invoice_date",to_date(spark_col("invoice_date"),"dd-MM-yyyy"))
df_stage = (df_stage
            .withColumn('Ano', year(df_stage.invoice_date))
            .withColumn('Mes', month(df_stage.invoice_date))
            .withColumn('Dia', dayofmonth(df_stage.invoice_date))
            .withColumn('Trimestre', quarter(df_stage.invoice_date))
           )
print("===============================================================")
print('CONTAGEM DAS LINHAS DO DATAFRAME DE GERAL: ', df_stage.count())
print("===============================================================")

df_stage = df_stage.withColumn("sales_price", regexp_replace(spark_col("sales_price"),",",".").cast(FloatType()))
df_stage.show(truncate=False, n=10)

# Criação das Chaves do Modelo

df_stage = df_stage.withColumn("DW_CLIENTE", sha2(concat_ws("", df_stage.customer, df_stage.customer_type), 256))
df_stage = df_stage.withColumn("DW_TEMPO", sha2(concat_ws("", df_stage.invoice_date, df_stage.Ano, df_stage.Mes, df_stage.Dia), 256))
df_stage = df_stage.withColumn("DW_LOCALIDADE", sha2(concat_ws("", df_stage.country, df_stage.state, df_stage.region_name, df_stage.city), 256))


df_stage.createOrReplaceTempView('stage')

#Criando a dimensão Cliente
dim_cliente = spark.sql('''
    SELECT DISTINCT
        DW_CLIENTE,
        customer,
        customer_type
    FROM stage    
''')

#Criando a dimensão Tempo
dim_tempo = spark.sql('''
    SELECT DISTINCT
        DW_TEMPO,
        invoice_date,
        Ano,
        Mes,
        Dia
    FROM stage    
''')

#Criando a dimensão Localidade
dim_localidade = spark.sql('''
    SELECT DISTINCT
        DW_LOCALIDADE,
        country,
        state,
        city,
        region_name
    FROM stage    
            where state IS NOT NULL and state <> ""
''')

#Criando a Fato Pedidios
ft_vendas = spark.sql('''
    SELECT 
        DW_CLIENTE,
        DW_LOCALIDADE,
        DW_TEMPO,
        sum(sales_price) as preco_venda
    FROM stage
    group by 
        DW_CLIENTE,
        DW_LOCALIDADE,
        DW_TEMPO
''')
dim_cliente.show(truncate=False)
dim_tempo.show(truncate=False)
dim_localidade.show(truncate=False)
ft_vendas.show(truncate=False)

# função para salvar os dados
def salvar_df(df, file):
    output = "/input/desafio_curso/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/desafio_curso/gold/"+file+".csv"
    print(rename)

    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(ft_vendas, 'ft_vendas')
salvar_df(dim_cliente, 'dim_cliente')
salvar_df(dim_tempo, 'dim_tempo')
salvar_df(dim_localidade, 'dim_localidade')

