from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("existence").getOrCreate()

df1_path = '/home/amirhosein/PycharmProjects/DFCompareData/'
df2_path = '/home/amirhosein/PycharmProjects/DFCompareData/modified'

df1 = spark.read.parquet(df1_path)
df2 = spark.read.parquet(df2_path)
df_columns = df1.columns
for col in df_columns:
    df2 = df2.withColumnRenamed(col, f'df2_{col}')

print(df2.schema)
merged = df1.join(df2, df1['id'] == df2['df2_id'], how='outer')
merged = merged.filter((merged['id'].isNull()) | (merged['df2_id'].isNull()))
merged = merged.withColumn('Lost_in',
                           when(merged['id'].isNull() & merged['df2_id'].isNull(), 'BOTH')
                           .when(merged['id'].isNull(), 'df1')
                           .when(merged['df2_id'].isNull(), 'df2')
                           )
merged.select('id','df2_id','Lost_in').show(truncate=False)
