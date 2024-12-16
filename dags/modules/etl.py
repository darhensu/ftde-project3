import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import psycopg2
import os
from sqlalchemy import create_engine
import pandas as pd

spark = SparkSession.builder \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
    .master("local") \
    .appName("PySpark_Postgres").getOrCreate()
    
def extract_transform_top_countries():
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
        
        
def load_top_countries(
    username, password, host, port, db
):
    df = pd.read_parquet('output/data_result_1')

    engine = create_engine(
        'mysql+mysqlconnector://4FFFhK9fXu6JayE.root:9v07S0pKe4ZYCkjE@gateway01.ap-southeast-1.prod.aws.tidbcloud.com:4000/project3',
        echo=False)
    df.to_sql(name='top_country', con=engine, if_exists='append')
    
    