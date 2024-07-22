from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws, array

spark = SparkSession.builder.appName('Compare_DF').getOrCreate()

df1_path = '/home/amirhosein/PycharmProjects/DFCompareData/'
df2_path = '/home/amirhosein/PycharmProjects/DFCompareData/modified'

df1 = spark.read.parquet(df1_path)
df2 = spark.read.parquet(df2_path)

columns = df1.columns
suffix_df1 = "_df1"
suffix_df2 = "_df2"

df1 = df1.select([col(c).alias(c + suffix_df1) for c in df1.columns])
df2 = df2.select([col(c).alias(c + suffix_df2) for c in df2.columns])

merged = df1.join(df2, df1['id_df1'] == df2['id_df2'], how='outer')

filter_expression = None
compare_columns = []
for col in columns:
    merged = merged.withColumn(f'compare_{col}', (merged[f'{col}_df1'] != merged[f'{col}_df2']))
    if filter_expression is None:
        filter_expression = merged[f'compare_{col}']
    else:
        filter_expression |= merged[f'compare_{col}']
    compare_columns.append(f'compare_{col}')


true_columns_array = array([when(merged[c] == True, c[8:]).otherwise(None) for c in compare_columns])

# Concatenate the column names into a single string
merged = merged.withColumn("differ_columns", concat_ws(",", true_columns_array))


merged.filter(filter_expression).select('id_df1','differ_columns').show()
print(df1.schema)
# print(merged.filter(merged.compare_balance == True).count())
