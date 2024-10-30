from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StringType,IntegerType,DateType,StructField
from transform import teste_transform
import sys


def main():
    spark = SparkSession.builder.master("local[*]").appName("Exemplo").config('spark.sql.repl.eagerEval.enabled', True).getOrCreate()

    #extract

    logins = spark.read.csv(r".\docs\LOGINS.csv",header=True, inferSchema = True, sep= ";") 
    
    #transform
    cor_select_user = teste_transform(df=logins)
    cor_select_user.write.format("console").save()

    #load
    cor_select_user.write.format("csv").mode("overwrite").save(sys.argv[1])


    spark.stop()

if __name__ == "__main__":
    main()