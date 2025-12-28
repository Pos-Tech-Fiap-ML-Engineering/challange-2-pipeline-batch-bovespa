import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, avg, date_format, to_date
from pyspark.sql.window import Window
from awsglue.dynamicframe import DynamicFrame

# --- 1. INICIALIZAÇÃO ---
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- 2. DADOS E CAMINHOS ---
bucket_name = "challange-2-pipeline-batch-bovespa-datalake"
s3_path_raw = f"s3://{bucket_name}/raw/b3_stocks/"
s3_path_refined = f"s3://{bucket_name}/refined/b3_stocks/"

# Infos do Catálogo
target_db = "db_b3_stocks"
target_table = "tb_b3_stocks"
aws_region = "us-east-1"

# --- 3. LEITURA E TRANSFORMACAO ---
print(f"Lendo dados de: {s3_path_raw}")
df = spark.read.parquet(s3_path_raw)

# Renomear
df = (
    df.withColumnRenamed("ticker", "codigo_acao")
      .withColumnRenamed("open", "valor_abertura")
      .withColumnRenamed("high", "valor_maximo")
      .withColumnRenamed("low", "valor_minimo")
      .withColumnRenamed("close", "valor_fechamento")
      .withColumnRenamed("volume", "volume_negociado")
)

# Tipagem e Filtros
df = df.withColumn("data", to_date(col("reference_date")))
df = df.filter(col("data").isNotNull())

# Cálculos
window_media = Window.partitionBy("codigo_acao").orderBy("data").rowsBetween(-2, 0)
df = df.withColumn("media_movel_3d", avg(col("valor_fechamento")).over(window_media))

window_hist = Window.partitionBy("codigo_acao")
df = df.withColumn("volume_medio_historico", avg(col("volume_negociado")).over(window_hist))

df = df.withColumn("amplitude_valor", col("valor_maximo") - col("valor_minimo"))

# Colunas de Partição
df = (
    df.withColumn("ano", date_format(col("data"), "yyyy"))
      .withColumn("mes", date_format(col("data"), "MM"))
      .withColumn("dia", date_format(col("data"), "dd"))
)

# --- 4. ESCRITA NO S3 ---
print(f"Salvando arquivos Parquet em: {s3_path_refined}")
dyf_final = DynamicFrame.fromDF(df, glueContext, "dyf_final")

glueContext.write_dynamic_frame.from_options(
    frame=dyf_final,
    connection_type="s3",
    connection_options={
        "path": s3_path_refined,
        "partitionKeys": ["ano", "mes", "dia", "codigo_acao"]
    },
    format="parquet",
    transformation_ctx="sink_refined"
)
print("✅ Arquivos salvos no S3.")

# --- 5. CADASTRO DA TABELA NO CATALOGO DO GLUE ---
glue_client = boto3.client("glue", region_name=aws_region)

try:
    glue_client.create_database(DatabaseInput={"Name": target_db})
except glue_client.exceptions.AlreadyExistsException:
    pass

# 5.2 Schema da Tabela (Baseado no DataFrame)
table_input = {
    "Name": target_table,
    "TableType": "EXTERNAL_TABLE",
    "Parameters": {"classification": "parquet"},
    "StorageDescriptor": {
        "Location": s3_path_refined,
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "Parameters": {"serialization.format": "1"}
        },
        "Columns": [
            {"Name": "valor_abertura", "Type": "double"},
            {"Name": "valor_maximo", "Type": "double"},
            {"Name": "valor_minimo", "Type": "double"},
            {"Name": "valor_fechamento", "Type": "double"},
            {"Name": "volume_negociado", "Type": "bigint"},
            {"Name": "data", "Type": "date"},
            {"Name": "media_movel_3d", "Type": "double"},
            {"Name": "volume_medio_historico", "Type": "double"},
            {"Name": "amplitude_valor", "Type": "double"}
        ],
        "Compressed": False,
        "StoredAsSubDirectories": False
    },
    "PartitionKeys": [
        {"Name": "ano", "Type": "string"},
        {"Name": "mes", "Type": "string"},
        {"Name": "dia", "Type": "string"},
        {"Name": "codigo_acao", "Type": "string"}
    ]
}

# 5.3 Cria ou Atualiza a Tabela
print(f"Atualizando catálogo: {target_db}.{target_table}...")
try:
    glue_client.create_table(DatabaseName=target_db, TableInput=table_input)
    print("✅ Tabela criada com sucesso!")
except glue_client.exceptions.AlreadyExistsException:
    # Se já existe, atualizamos o schema para garantir
    glue_client.update_table(DatabaseName=target_db, TableInput=table_input)
    print("✅ Tabela atualizada com sucesso!")

# Recarregar Partições (Como salvamos partições novas no S3, precisamos avisar o Glue)
print("Reparando partições...")
try:
    spark.sql(f"MSCK REPAIR TABLE {target_db}.{target_table}")
except Exception as e:
    # Fallback caso o Spark SQL não consiga acessar o catálogo diretamente nessa sessão
    print(f"Aviso: Execute 'MSCK REPAIR TABLE' no Athena para ver todas as partições. ({e})")

# --- 6. Finalização ---
job.commit()
print("Job Finalizado! Pode ir pro Athena.")