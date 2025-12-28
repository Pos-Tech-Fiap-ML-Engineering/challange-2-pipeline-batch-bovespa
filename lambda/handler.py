import json
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    glue_job_name = "challange-2-pipeline-batch-bovespa-glue-etl"
    glue_client = boto3.client('glue')

    logger.info("Iniciando processamento de mensagens da fila SQS...")

    # O evento vem do SQS, que contém a notificação do S3 dentro dele
    for sqs_record in event['Records']:
        try:
            # O corpo da mensagem SQS é uma string JSON (o evento do S3)
            s3_event_body = json.loads(sqs_record['body'])

            # Pula eventos de teste do S3
            if 'Event' in s3_event_body and s3_event_body['Event'] == 's3:TestEvent':
                logger.info("Evento de teste do S3 ignorado.")
                continue

            # Itera sobre os arquivos notificados pelo S3
            if 'Records' in s3_event_body:
                for s3_record in s3_event_body['Records']:
                    bucket = s3_record['s3']['bucket']['name']
                    key = s3_record['s3']['object']['key']

                    logger.info(f"Arquivo detectado via SQS: s3://{bucket}/{key}")

                    # --- DISPARO DO GLUE ---
                    # Tenta iniciar o Job. Se já estiver rodando, captura o erro e segue o baile.
                    try:
                        response = glue_client.start_job_run(JobName=glue_job_name)
                        logger.info(f"✅ Job Glue disparado com sucesso! Run ID: {response['JobRunId']}")
                    except glue_client.exceptions.ConcurrentRunsExceededException:
                        logger.warning(
                            f"⚠️ O Job {glue_job_name} já está rodando. O arquivo {key} será processado na execução atual.")
                    except Exception as e:
                        logger.error(f"❌ Erro ao tentar iniciar o Glue: {e}")
                        raise e  # Lança erro para a mensagem voltar pra fila e tentar depois
            else:
                logger.warning("Mensagem SQS recebida sem registros S3 válidos.")

        except Exception as e:
            logger.error(f"Erro fatal processando registro SQS: {e}")
            raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Ciclo de processamento concluído.')
    }