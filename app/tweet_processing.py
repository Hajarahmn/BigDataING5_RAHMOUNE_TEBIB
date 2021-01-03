from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming import StreamingContext
from pyspark import SparkContext

#spark = SparkSession.builder.appName('PythonStreamingDirectKafkaWordCount').getOrCreate()
spark = SparkSession \
         .builder \
         .appName("PythonStreamingDirectKafkaWordCount") \
         .config("spark.jars", "/home/shuya10/BigDataING5_RAHMOUNE_TEBIB/app/kafka-clients-1.1.0.jar") \
         .config("spark.executor.extraClassPath", "/home/shuya10/BigDataING5_RAHMOUNE_TEBIB/app/kafka-clients-1.1.0.jar") \
         .config("spark.executor.extraLibrary", "/home/shuya10/BigDataING5_RAHMOUNE_TEBIB/app/kafka-clients-1.1.0.jar") \
         .config("spark.driver.extraClassPath", "/home/shuya10/BigDataING5_RAHMOUNE_TEBIB/app/kafka-clients-1.1.0.jar") \
         .config("java.security.auth.login.config", "/home/shuya10/BigDataING5_RAHMOUNE_TEBIB/conf/kafka_jaas.conf") \
         .getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc,1)    


KAFKA_TOPIC_NAME_CONS = "ece_2020_fall_app_2"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'kfk-brk-2.au.adaltas.cloud:6667'

if __name__ == "__main__":
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
    df1.writeStream \
       .option("path","/home/shuya10/BigDataING5_RAHMOUNE_TEBIB/app/") \
       .format("csv") \
       .option("checkpointLocation","chkpint_directory") \
       .outputMode("append") \
       .start()
    ssc.awaitTermination()

#        .config("spark.jars", "/home/shuya10/BigDataING5_RAHMOUNE_TEBIB/app/kafka-clients-1.1.0.jar") \
#        .config("spark.executor.extraClassPath", "/home/shuya10/BigDataING5_RAHMOUNE_TEBIB/app/kafka-clients-1.1.0.jar") \
#        .config("spark.executor.extraLibrary", "/home/shuya10/BigDataING5_RAHMOUNE_TEBIB/app/kafka-clients-1.1.0.jar") \
#        .config("spark.driver.extraClassPath", "/home/shuya10/BigDataING5_RAHMOUNE_TEBIB/app/kafka-clients-1.1.0.jar") \