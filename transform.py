import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType



def teste_transform(df):
    # Adicionando uma coluna "status" com base no estado

    transform_email = (
        df
        .withColumn("email", F.split(F.col("email"),"@").getItem(0))
        .withColumn("email" ,F.regexp_replace(F.col("email"),"[^a-zA-Z]",""))
        .withColumn("email" , F.initcap("email"))
    )

    transform_tel = (
        transform_email
        .withColumn("telefone", F.regexp_replace(F.col("telefone"), "(\D)",""))
    )

    idade = (
        transform_tel.withColumn("idade", F.datediff(F.current_date(), F.col("data_de_nascimento"))/365)
        .withColumn("idade", F.col("idade").cast("int"))
    )

    logins = (
        idade
        .select(
            F.col("email").alias("nome"), F.col("idade"), F.col("cpf"), F.col("estado"), F.col("cor_favorita"), F.col("profissao"), F.col("telefone")
        )
    )
    
    return logins