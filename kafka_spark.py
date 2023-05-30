from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def create_name_cpf():
    name = ['João', 'Maria', 'José', 'Ana', 'Pedro', 'Paulo', 'Carlos', 'Luiz', 'Marcos', 'Antônio']
    cpf = ['123.456.789-00', '987.654.321-00', '123.123.123-00', '456.456.456-00', '789.789.789-00', '321.321.321-00', '654.654.654-00', '987.987.987-00', '159.159.159-00', '753.753.753-00']
    return name, cpf

def query_cpf(person):
    name, cpf = create_name_cpf()
    for i in range(len(name)):
        if name[i] == person:
            return cpf[i]

# Inicialize a SparkSession
spark = SparkSession.builder \
    .appName("Teste") \
    .config('spark.jars.packages', "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0") \
    .config('spark.streaming.kafka.maxRatePerPartition', 1000) \
    .config('spark.default.parallelism', 2) \
    .config('spark.driver.memory', '4g') \
    .getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sc, 1)

kvs = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'teste':1})
lines = kvs.map(lambda x: x[1]+', '+query_cpf(x[1]))
lines.pprint()
ssc.start()
ssc.awaitTermination()