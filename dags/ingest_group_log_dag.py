from airflow.decorators import dag, task
import pendulum
import boto3
from airflow.providers.vertica.hooks.vertica import VerticaHook
import os

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

SQL_FILE = os.path.join(os.path.dirname(__file__), "./sql/create_tables.sql")
LOAD_GROUP_LOG_TO_STG = os.path.join(os.path.dirname(__file__), "./sql/ingest_stg_group_log.sql")
CREATE_TABLE_GROUP_LOG = os.path.join(os.path.dirname(__file__), "./sql/create_group_log_table.sql")


def get_s3_client():
    session = boto3.session.Session()
    return session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 7, 13),
    catchup=False,
    tags=["sprint6", "ingest", "sprint_project"],
)

def sprint_project():
    
    @task
    def fetch_group_log_csv():
        s3 = get_s3_client()
        s3.download_file(
            Bucket="sprint6",
            Key="group_log.csv",
            Filename="/data/group_log.csv"
        )
        return "/data/groups.csv"
    
    @task
    def create_group_log_table(sql_file: str):
        hook = VerticaHook(vertica_conn_id="vertica_conn")

        with open(sql_file, "r") as f:
            query = f.read()
        hook.run(query)    

    @task
    def load_group_log_to_stg(sql_file: str):
        hook = VerticaHook(vertica_conn_id="vertica_conn")

        with open(sql_file, "r") as f:
            query = f.read()
        hook.run(query)    


    # ----- DAG graph -----
    download_group_log = fetch_group_log_csv()
    create_table = create_group_log_table(CREATE_TABLE_GROUP_LOG)
    ingest_to_stg_group_log = load_group_log_to_stg(LOAD_GROUP_LOG_TO_STG)

    [download_group_log, create_table] >> ingest_to_stg_group_log

_ = sprint_project()