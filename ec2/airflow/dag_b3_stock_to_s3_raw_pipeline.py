from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import boto3
from botocore.exceptions import ClientError

# --- CONFIGURATION ---
BUCKET_NAME = "challange-2-pipeline-batch-bovespa-datalake"
GLUE_CRAWLER_ROLE_ARN = "arn:aws:iam::355262196527:role/challange-2-pipeline-batch-bovespa-glue"
GLUE_DB_NAME = "db_b3_stocks_raw"
CRAWLER_NAME = "crawler_raw_b3_stock_daily_update"
TICKER_LIST = ['PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'BBDC4.SA', 'WEGE3.SA']
AWS_REGION = "us-east-1"

AWS_TAGS = {
    "Project": "challange-2-pipeline-batch-bovespa",
    "ManagedBy": "Airflow"
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}


# --- ETL FUNCTIONS ---

def extract_data(tickers, execution_date):
    """Fetches stock data for a specific single day using Pandas/Yfinance."""
    print(f"--- EXTRACT: Fetching data for {execution_date} ---")

    dt_obj = datetime.strptime(execution_date, '%Y-%m-%d')
    next_day = (dt_obj + timedelta(days=1)).strftime('%Y-%m-%d')

    try:
        # Baixa os dados
        df = yf.download(
            tickers=tickers,
            start=execution_date,
            end=next_day,
            group_by='ticker',
            progress=False
        )
        return df
    except Exception as e:
        print(f"❌ Error connecting to yfinance: {e}")
        raise


def transform_data(df_raw, execution_date):
    """
    Limpa os dados, remove a coluna de Data problemática (Nanosegundos)
    e adiciona a data de referência (Partition Key).
    """
    if df_raw.empty:
        print("⚠️ No data found (likely weekend or holiday).")
        return None

    try:
        # 1. Empilha os Tickers para ficar tabular
        df_clean = df_raw.stack(level=0).reset_index()

        # 2. Renomeia colunas dinâmicas
        cols = df_clean.columns
        # O yfinance geralmente cria uma coluna 'Date' no reset_index ou 'level_1'
        if 'level_1' in cols:
            df_clean.rename(columns={'level_1': 'Ticker'}, inplace=True)
        elif 'Ticker' not in cols:
            # Tenta pegar a segunda coluna que geralmente é o Ticker
            df_clean.rename(columns={cols[1]: 'Ticker'}, inplace=True)

        # 3. CRÍTICO: Remover a coluna 'Date' original que tem nanosegundos
        # O Glue odeia essa coluna vinda do Pandas. Vamos removê-la.
        if 'Date' in df_clean.columns:
            df_clean = df_clean.drop(columns=['Date'])
            print("✅ Coluna 'Date' (nanosegundos) removida com sucesso.")

        # 4. Adiciona a data de referência (String YYYY-MM-DD)
        # Essa será nossa chave de partição e data oficial
        df_clean['reference_date'] = execution_date

        return df_clean
    except Exception as e:
        raise ValueError(f"Error transforming data: {e}")


def run_etl_process(**context):
    """Main wrapper function."""
    execution_date = context['ds']

    # 1. Extract
    df_raw = extract_data(TICKER_LIST, execution_date)

    # 2. Transform
    df_clean = transform_data(df_raw, execution_date)

    if df_clean is None:
        raise AirflowSkipException("No data found for this date.")

    # 3. Load to S3 (Usando Pandas mas forçando compatibilidade)
    s3_path = f"s3://{BUCKET_NAME}/raw/b3_stocks/"
    print(f"--- LOAD: Uploading to {s3_path} ---")

    # engine='pyarrow' é o padrão e mais rápido
    # index=False garante que não vamos salvar índices numéricos estranhos
    df_clean.to_parquet(
        s3_path,
        index=False,
        partition_cols=['reference_date'],
        compression='snappy'
    )
    print("✅ ETL Success.")


# --- INFRASTRUCTURE CHECK ---

def check_and_create_glue_resources():
    """Garante que o Database e Crawler existam antes de rodar."""
    glue_client = boto3.client('glue', region_name=AWS_REGION)

    try:
        glue_client.get_database(Name=GLUE_DB_NAME)
        print(f"✅ Database '{GLUE_DB_NAME}' already exists.")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"⚠️ Creating Database '{GLUE_DB_NAME}'...")
        glue_client.create_database(
            DatabaseInput={'Name': GLUE_DB_NAME, 'Description': 'Database for Raw B3 Stocks'},
            Tags=AWS_TAGS
        )

    try:
        glue_client.get_crawler(Name=CRAWLER_NAME)
        print(f"✅ Crawler '{CRAWLER_NAME}' already exists.")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"⚠️ Creating Crawler '{CRAWLER_NAME}'...")
        s3_target = f"s3://{BUCKET_NAME}/raw/b3_stocks/"
        glue_client.create_crawler(
            Name=CRAWLER_NAME,
            Role=GLUE_CRAWLER_ROLE_ARN,
            DatabaseName=GLUE_DB_NAME,
            Targets={'S3Targets': [{'Path': s3_target}]},
            TablePrefix="tb_",
            SchemaChangePolicy={'UpdateBehavior': 'UPDATE_IN_DATABASE', 'DeleteBehavior': 'DELETE_FROM_DATABASE'},
            Tags=AWS_TAGS
        )


# --- DAG DEFINITION ---

with DAG(
        'b3_stock_to_s3_raw_pipeline',
        default_args=default_args,
        description='Daily B3 Stocks ETL + Glue Infra',
        schedule_interval='0 22 * * *',
        catchup=True,
        max_active_runs=1,
        tags=['b3', 's3', 'raw'],
) as dag:
    task_setup_infra = PythonOperator(
        task_id='setup_glue_infrastructure',
        python_callable=check_and_create_glue_resources
    )

    task_etl = PythonOperator(
        task_id='etl_process_yfinance',
        python_callable=run_etl_process,
        provide_context=True
    )

    task_trigger_crawler = GlueCrawlerOperator(
        task_id='update_glue_catalog',
        config={'Name': CRAWLER_NAME},
        aws_conn_id='aws_default',
        region_name=AWS_REGION,
        wait_for_completion=True
    )

    task_setup_infra >> task_etl >> task_trigger_crawler