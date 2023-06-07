from datetime import datetime,timedelta,date
from airflow import models,DAG

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator, DataprocSubmitPySparkJobOperator, DataprocDeleteClusterOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
import os

current_date=datetime.now().strftime("%Y-%m-%d")

bucket_path=os.environ.get('BUCKET_PATH')
bucket_name=os.environ.get('BUCKET_NAME')

pyspark_job=os.environ.get('pyspark_job')
cluster_name=os.environ.get('cluster_name')
region=os.environ.get("region")
destination_project_dataset_table=os.environ.get("destination_project_dataset_table")
project_id=os.environ.get("project_id")
owner=os.environ.get("owner")
DEFAULT_DAG_args={
    'owner':owner,
    'depends_on_past':False,
    'start_date':datetime.utcnow(),
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5),
    'project_id':project_id,
    'scheduled_interval':"30 2 * * *"
}
with DAG("esc_etl",default_args=DEFAULT_DAG_args) as dag:

    submit_pyspark = DataprocSubmitPySparkJobOperator(
        task_id="run_pyspark",
        main=pyspark_job,
        cluster_name=cluster_name,
        region=region
    )
    bq_load=GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=bucket_name,
        source_objects="outputs/"+current_date+"_/part-*.csv",
        destination_project_dataset_table=destination_project_dataset_table,
        autodetect=True,
        skip_leading_rows=1,
        source_format='CSV',
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        max_bad_records=0
    )
    delete_cluster = DataprocDeleteClusterOperator(

        task_id="delete-cluster",
        cluster_name=cluster_name,
        region=region,
        trigger_rule=TriggerRule.ALL_DONE
    )
    delete_transformed_files = BashOperator(
        task_id="delete_Files",
        bash_command="gsutil -m rm -r {bucket}/outputs/*".format(bucket=bucket_name)

    )
    submit_pyspark.dag=dag

    submit_pyspark.set_downstream([bq_load,delete_cluster])
    delete_cluster.set_downstream(delete_transformed_files)
