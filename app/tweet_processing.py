from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName('PythonStreamingDirectKafkaWordCount').getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 20)

import os


KAFKA_TOPIC_NAME_CONS = "ece_2020_fall_app_2"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'kfk-brk-1.au.adaltas.cloud:6667','kfk-brk-2.au.adaltas.cloud:6667','kfk-brk-3.au.adaltas.cloud:6667'
df = spark.readStream \
     .format("kafka") \
     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
     .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
     .option("startingOffsets", "earliest") \
     .option("kafka.security.protocol","SASL_PLAINTEXT")\
     .option("kafka.sasl.mechanism","GSSAPI")\
     .option("kafka.sasl.kerberos.service.name","kafka")\
     .load()

df1 = df.selectExpr( "CAST(value AS STRING)")
ssc.start()
ssc.awaitTermination()