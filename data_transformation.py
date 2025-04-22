


import pyspark


from pyspark import SparkContext
from pyspark import SQLContext





from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession





import pandas as pd
sc=SparkContext()
spark= SQLContext(sc)
file_path=os.environ.get('file_path')
output_path=os.environ.get('output_path')
esc_data= spark.read.option("multiline","true").json(file_path)





esc_data=esc_data.dropDuplicates()





s=esc_data.count()
print(s)





#remove spaces between col name
esc_data = esc_data.toDF(*[c.replace(" ", "_") for c in esc_data.columns])

esc_data.printSchema()




esc_data = esc_data.withColumnRenamed('Points______','Points')
esc_data = esc_data.withColumnRenamed('(semi-)_final','Round')

esc_data.printSchema()





#missing values

from pyspark.sql.functions import col, sum

# Count the number of null values in each column
null_counts = esc_data.agg(*[sum(col(c).isNull().cast("integer")).alias(c) for c in esc_data.columns])


null_counts.show()





import datetime
current_date=datetime.date.today()
current_date=str(current_date)

output_path=output_path+current_date+"_"
esc_data.repartition(1).write.options(header=True).csv(output_path)

