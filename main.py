from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName('Tema1').getOrCreate()
df_pyspark = spark.read.csv('Erasmus.csv', header=True)
def erasmus_data_filtering():
    df_student_cnt = df_pyspark.groupBy(["Receiving Country Code", "Sending Country Code"]) \
        .count()
    df_student_cnt = df_student_cnt.orderBy("Receiving Country Code", "Sending Country Code")

    print('\n\n')
    print("~ Number of students that went on an Erasmus mobility, based on every Receiving Country Code")
    df_student_cnt.show(n=df_student_cnt.count())

    print('\n\n')
    print("~ Number of students that went on an Erasmus mobility, based on a Receiving Country Code from the following: LV, MK, MT")
    df_filtered = df_student_cnt.where(col("Receiving Country Code").isin(["LV", "MK", "MT"]))
    df_filtered.show(n=50)

    return df_filtered
erasmus_data_filtering()