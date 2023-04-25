import requests
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json 
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

 

#API Request

url = "https://api.spacexdata.com/v5/launches/"

# Realiza uma requisição GET para a API e armazena a resposta na variável 'response'
response = requests.get(url)
                                             
# Verifica se a resposta foi bem sucedida (status code 200 indica sucesso)
if response.status_code == 200:
    # Se a resposta foi bem sucedida, você pode acessar o conteúdo da resposta
    data = response.json() # assume que a resposta é um JSON
else:
    # Se a resposta não foi bem sucedida, imprime uma mensagem de erro
    print("Erro ao realizar requisição. Status code: {}".format(response.status_code))

#Definindo estrutura que será usada para criar o spark dataframe 
schema = StructType([
  StructField("fairings", StructType([
    StructField("reused", BooleanType(), True),
    StructField("recovery_attempt", BooleanType(), True),
    StructField("recovered", BooleanType(), True),
    StructField("ships", ArrayType(StringType()), True)
  ]), True),
  StructField("links", StructType([
    StructField("patch", StructType([
      StructField("small", StringType(), True),
      StructField("large", StringType(), True)
    ]), True),
    StructField("reddit", StructType([
      StructField("campaign", StringType(), True),
      StructField("launch", StringType(), True),
      StructField("media", StringType(), True),
      StructField("recovery", StringType(), True)
    ]), True),
    StructField("flickr", StructType([
      StructField("small", ArrayType(StringType()), True),
      StructField("original", ArrayType(StringType()), True)
    ]), True),
    StructField("presskit", StringType(), True),
    StructField("webcast", StringType(), True),
    StructField("youtube_id", StringType(), True),
    StructField("article", StringType(), True),
    StructField("wikipedia", StringType(), True)
  ]), True),
  StructField("static_fire_date_utc", StringType(), True),
  StructField("static_fire_date_unix", LongType(), True),
  StructField("net", BooleanType(), True),
  StructField("window", IntegerType(), True),
  StructField("rocket", StringType(), True),
  StructField("success", BooleanType(), True),
  StructField("failures", ArrayType(StructType([
    StructField("time", IntegerType(), True),
    StructField("altitude", IntegerType(), True),
    StructField("reason", StringType(), True)
  ])), True),
  StructField("details", StringType(), True),
  StructField("crew", ArrayType(StringType()), True),
  StructField("ships", ArrayType(StringType()), True),
  StructField("capsules", ArrayType(StringType()), True),
  StructField("payloads", ArrayType(StringType()), True),
  StructField("launchpad", StringType(), True),
  StructField("flight_number", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("date_utc", StringType(), True),
  StructField("date_unix", LongType(), True),
  StructField("date_local", StringType(), True),
  StructField("date_precision", StringType(), True),
  StructField("upcoming", BooleanType(), True),
  StructField("cores", ArrayType(StructType([
    StructField("core", StringType(), True),
    StructField("flight", IntegerType(), True),
    StructField("gridfins", BooleanType(), True),
    StructField("legs", BooleanType(), True),
    StructField("reused", BooleanType(), True),
    StructField("landing_attempt", BooleanType(), True),
    StructField("landing_success", BooleanType(), True),
    StructField("landing_type", StringType(), True),
    StructField("landpad", StringType(), True)
  ])), True),
  StructField("auto_update", BooleanType(), True),
  StructField("tbd", BooleanType(), True),
  StructField("launch_library_id", StringType(), True),
  StructField("id", StringType(), True)
])

#Criando spark dataframe 
df1 = spark.createDataFrame(data, schema=schema)

#Escrevendo na camada bronze
df1.write.mode("overwrite").format("delta").save("dbfs:/FileStore/bronze/spacex")

#Lendo da Bronze para escrever na Silver

df_silver = spark.sql("select * from delta.`dbfs:/FileStore/bronze/spacex`")

#Criando dimensão de tempo
df_silver.createOrReplaceTempView("temp_silver")
dim_time = spark.sql("""SELECT  ID 
                               ,YEAR(DATE_UTC) AS YEAR
                               ,MONTH(DATE_UTC) AS MONTH
                               ,DAY(DATE_UTC) AS DAY
                               ,SUBSTRING(DATE_UTC,12,19) AS HOUR
                         FROM temp_silver
                     """)

#Formatando a dimensão de tempo para deixar mais detalhado
dim_time = dim_time.withColumn("HOUR", date_format(col("HOUR"), "hh:MM:ss"))

#Dropando as colunas desnecessárias

columns = ("static_fire_date_unix","date_utc", "date_unix")
df_silver = df_silver.drop(*columns)

#Salvando na camada silver 
df_silver.write.mode("overwrite").format("delta").save("dbfs:/FileStore/silver/spacex_data")
dim_time.write.mode("overwrite").format("delta").save("dbfs:/FileStore/silver/time_dimension")

#Lendo da silver e escrevendo na Gold
df_gold = spark.sql("""
                      SELECT 
                            ID
                            ,fairings
                            ,static_fire_date_utc
                            ,rocket
                            ,failures
                            ,details
                            ,ships
                            ,capsules
                      FROM delta.`dbfs:/FileStore/silver/spacex_data`
                      WHERE success = 'false'
                    """)



dim_time = spark.sql("""
                      SELECT 
                            *
                      FROM delta.`dbfs:/FileStore/silver/time_dimension`
                    """)

#Criando informação de aeronavas que falharam
df_failure = df_gold.join(dim_time, df_gold.ID == dim_time.ID, "inner").drop(dim_time.ID)
df_failure.write.mode("overwrite").format("delta").save("dbfs:/FileStore/gold/failure_table") 

#Pegando as aeronaves recuperadas
#Accessing only the recovered spacecrafts

df_recovered = spark.sql("""
                            SELECT 
                                  ID
                                  ,fairings.recovered
                                  ,static_fire_date_utc
                                  ,rocket
                                  ,failures
                                  ,details
                                  ,ships
                                  ,capsules
                            FROM delta.`dbfs:/FileStore/silver/spacex_data`
                            WHERE fairings.recovered = true
                         """)

#Cruzando com informação de tempo
df_recovered = df_recovered.join(dim_time, df_recovered.ID == dim_time.ID, "inner").drop(dim_time.ID)

#Salvando na Gold
df_recovered.write.mode("overwrite").format("delta").save("dbfs:/FileStore/gold/recovered_table")

