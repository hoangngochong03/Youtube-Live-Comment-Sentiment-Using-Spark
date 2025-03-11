

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import time,pandas
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType,TimestampType
import pyspark
import re
from pyspark.ml import  PipelineModel
from pyspark.sql.functions import udf
import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
import os # chuyển os qua xài môi trường python myenv trùng với worker
 
os.environ["PYTHONPATH"] = "./sparkenv/Lib/site-packages"  # replace path with your python env

scala_version = '2.12'  # Scala version
spark_version = '3.5.3' # Spark version

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.6.0'  # Kafka version
]

PYTHON_EXECUTABLE = "//sparkenv/Scripts/python.exe"# replace path with your python env
# Replace path file that install in your folder
sc = SparkSession.builder \
    .master("local[*]") \
    .appName("youtube-Streaming") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.pyspark.python", PYTHON_EXECUTABLE) \
    .config("spark.pyspark.driver.python", PYTHON_EXECUTABLE) \
    .config("spark.executorEnv.PYTHONPATH", PYTHON_EXECUTABLE) \
    .config("spark.executorEnv.PYSPARK_PYTHON", PYTHON_EXECUTABLE) \
    .config("spark.driver.extraClassPath", "./spark-3.5.3-bin-hadoop3/jars") \
    .config("spark.executor.extraClassPath", "./spark-3.5.3-bin-hadoop3/jars") \
    .config("spark.local.dir", "C:/sparktmp") \
    .config("spark.hadoop.io.native.lib", "false") \
    .getOrCreate()


# KAFKA
topic_name = 'Youtubelive-comments'
kafka_server = 'localhost:9092'

# Read data from Kafka
kafkaDf = sc.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", topic_name) \
    .option("startingOffsets", "latest") \
    .load()
#Load model for sentiment
model=PipelineModel.load("models/randomforest_model")
#Define Schema
schema = StructType([
    StructField("video_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("raw_comment", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])

parsedDf = kafkaDf \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select(
        col("data.video_id"),
        col("data.author"),
        col("data.raw_comment"),
        col("data.timestamp")
    )
# Process abbreviation word
abbreviation_df=sc.read.csv(r".\data\dictionary\abb_dict_normal.csv",header=True,inferSchema=True)

abbreviation_dict = {row["abbreviation"]: row["meaning"] for row in abbreviation_df.collect()}

def abbreviation(text):
    if text is None:
        return None  

    words = text.split()  
    words = [abbreviation_dict.get(word, word) for word in words] 
    return " ".join(words)  
process_abbreviations_udf = udf(abbreviation, StringType())

# Remove Emoji
def remove_emoji(text):
    emoji_pattern = re.compile(
        "["
        u"\U0001F600-\U0001F64F"  
        u"\U0001F300-\U0001F5FF"  
        u"\U0001F680-\U0001F6FF"  
        u"\U0001F1E0-\U0001F1FF"  
        "]+",
        flags=re.UNICODE
    )
    return emoji_pattern.sub(r'', text) if text else text
remove_emoji_udf = udf(remove_emoji, StringType())

# Remove icon in youtube (Format icon in kafka when retrieve data   :rolling_on_the_floor_laughing: )
def remove_text_within_colons(text):
    if text:
        return re.sub(r':[^:]+:', '', text)
    return text
remove_text_udf = udf(remove_text_within_colons, StringType())


cleanedDf = parsedDf.withColumn("cleanemoji_comment", remove_emoji_udf(col("raw_comment")))

cleanedDf = cleanedDf.withColumn("clean_comment", remove_text_udf(col("cleanemoji_comment")))

cleanedDf = cleanedDf.withColumn("Comment", process_abbreviations_udf(col("clean_comment")))
# Import Data into postgres to storage ( replace with your postgres connection)
import psycopg2
def create_db_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="*****", 
        host="localhost",
        port="5432"
    )
# Process data from kafka by batch
def process_batch(batch_df, batch_id):
    predictions = model.transform(batch_df.filter(batch_df.Comment.isNotNull()))
    pandas_df = predictions.toPandas()
    column_order=["video_id","author","timestamp","raw_comment","Comment","prediction"]
    pandas_df=pandas_df.loc[:, column_order]
    # load data to database
    conn = create_db_connection()
    cursor = conn.cursor()
    try:
        for row in predictions.collect():
            cursor.execute(
                "INSERT INTO youtube_live_comment (video_id,author,time_comment,raw_comment,processed_comment,sentiment) VALUES (%s, %s,%s, %s, %s, %s)",
                (row["video_id"], row["author"], row["timestamp"],row["raw_comment"], row["Comment"],row["prediction"] )
            )
        conn.commit()
    except Exception as e:
        print("Error import data:",e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
    print(f"Batch {batch_id}:")
    print(pandas_df)
# Show batch data in terminal with each 60 seconds
query = cleanedDf.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .trigger(processingTime="60 seconds") \
    .start()
query.awaitTermination()

