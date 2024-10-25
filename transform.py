import pyspark.sql.functions as F

def teste_transform(df):

    logins = (
       df.select(
       F.col("email"), F.col("cpf"),F.col("estado"),F.col("cor_favorita")
       ).where(F.col("cor_favorita") == "Roxo")
    )
    return logins
