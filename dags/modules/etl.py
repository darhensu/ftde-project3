import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import os

spark = SparkSession.builder \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
    .master("local") \
    .appName("PySpark_Postgres").getOrCreate()
    
def extract_load_top_countries():
    df_city = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/data_warehouse") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "city") \
        .option("user", "postgres") \
        .option("password", "password.1").load()
    df_city.createOrReplaceTempView("city")
    
    df_country = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/data_warehouse") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "country") \
        .option("user", "postgres") \
        .option("password", "password.1").load()
    df_country.createOrReplaceTempView("country")

    df_address = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/data_warehouse") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "customer") \
        .option("user", "postgres") \
        .option("password", "password.1").load()
    df_address.createOrReplaceTempView("address")

    df_result = spark.sql('''
        SELECT 
            country,
            COUNT(country) as total,
            current_date as date,
            'suhendar' as data_owner
        FROM country as co
        INNER JOIN city as ci
            ON ci.country_id = co.country_id
        GROUP BY country          
        ''')

    df_result.write.mode('overwrite') \
        .partitionBy('date') \
        .option('compression', 'snappy') \
        .option('partitionOverwriteMode', 'dynamic') \
        .save('output/data_result_1')
    
    