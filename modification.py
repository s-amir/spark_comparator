from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.functions import monotonically_increasing_id


spark = SparkSession.builder.appName('Compare_DF').getOrCreate()

df_source = '/home/amirhosein/Downloads/BankCustomerData.csv'
df1_path = '/home/amirhosein/PycharmProjects/DFCompareData/'
df2_path = '/home/amirhosein/PycharmProjects/DFCompareData/modified'

df1 = spark.read.csv(df_source, header=True)
df1=df1.withColumn('id', monotonically_increasing_id())
#delete id = 3 from df1
df1_deleted = df1.filter(df1['id'] != 3)
#change balance number in df2
df2 = df1.withColumn('balance', when(df1['age'] == '58', 0).otherwise(df1['balance']))
df2 = df2.withColumn('job', when(df1['age'] == '58', 'BI_KAAAR').otherwise(df1['job']))
df1_deleted.write.parquet(df1_path, mode='overwrite')
df2.write.parquet(df2_path,mode='overwrite')


spark.read.parquet(df1_path).filter(col('id')==3).show()
spark.read.parquet(df2_path).filter(col('id')==3).show()