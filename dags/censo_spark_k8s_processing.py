from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark.kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark.kubernetes import SparkKubernetesSensor
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3

aws_access_key_id = Variable.get('aws_access_key_id')
aws_secret_access_key = Variable.get('aws_secrets_access_key')
glue = boto3.client('glue', region_name='us-east-1',
                    aws_access_key_id=aws_access_key_id.
                    aws_secret_access_key=aws_secret_access_key)

from airflow.utils.dates import days_ago

def trigger_crawler_final_func():
        glue.start_crawler(Name='censo_final_crawler')

with DAG(
    'censo_batch_spark_k8s',
    default_args={
        'owner': 'asaito2001',
        'depends_on_past': False,
        'email': ['asaito2001@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'max_active_runs': 1
    },
    description='submit spark-pi as SparkApplication on kubernetes',
    schedule_interval="0 */2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=['spark', 'kubernetes', 'batch', 'censo']
) as dag:
    converte_aluno_parquet = SparkKubernetesOperator(
        task_id='converte_aluno_parquet',
        namespace="airflow",
        application_file="censo_converte_aluno_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )

    converte_aluno_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_aluno_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_aluno_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default"
    )

    converte_docente_parquet = SparkKubernetesOperator(
        task_id='converte_docente_parquet',
        namespace="airflow",
        application_file="censo_converte_docente_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )

    converte_docente_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_docente_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_docente_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default"
    )

    converte_curso_parquet = SparkKubernetesOperator(
        task_id='converte_curso_parquet',
        namespace="airflow",
        application_file="censo_converte_curso_parquet.yaml",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True
    )

    converte_curso_parquet_monitor = SparkKubernetesSensor(
        task_id='converte_curso_parquet_monitor',
        namespace="airflow",
        application_name="{{ task_instance.xcom_pull(task_ids='converte_curso_parquet')['metadata']['name'] }}",
        kubernetes_conn_id="kubernetes_default"
    )


#    agrega_algo = SparkKubernetesOperator(
#        task_id='agrega_algo',
#        namespace="airflow",
#        application_file="censo_agrega_algo.yaml",
#        kubernetes_conn_id="kubernetes_default",
#        do_xcom_push=True
#    )

#    agrega_algo_monitor = SparkKubernetesSensor(
#        task_id='agrega_algo_monitor',
#        namespace="airflow",
#        application_name="{{ task_instance.xcom_pull(task_ids='agrega_algo')['metadata']['name'] }}",
#        kubernetes_conn_id="kubernetes_default"
#    )

#    join_final = SparkKubernetesOperator(
#        task_id='join_final',
#        namespace="airflow",
#        application_file="censo_join_final.yaml",
#        kubernetes_conn_id="kubernetes_default",
#        do_xcom_push=True
#    )

#    join_final_monitor = SparkKubernetesSensor(
#        task_id='join_final_monitor',
#        namespace="airflow",
#        application_name="{{ task_instance.xcom_pull(task_ids='join_final')['metadata']['name'] }}",
#        kubernetes_conn_id="kubernetes_default"
#    )

    trigger_crawler_final = PythonOperator(
        task_id='trigger_crawler_final',
        python_callable=trigger_crawler_final_func
    )

converte_aluno_parquet >> converte_aluno_parquet_monitor
converte_aluno_parquet_monitor >> converte_curso_parquet >> converte_curso_parquet_monitor
converte_curso_parquet_monitor >> converte_docente_parquet >> converte_docente_parquet_monitor
converte_docente_parquet_monitor >> trigger_crawler_final