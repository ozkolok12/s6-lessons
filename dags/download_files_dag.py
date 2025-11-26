from airflow.decorators import dag, task
import pendulum
import boto3
from airflow.providers.vertica.hooks.vertica import VerticaHook
import os
from airflow.models import Variable


AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")

SQL_FILE = os.path.join(os.path.dirname(__file__), "./sql/create_tables.sql")
LOAD_USERS_TO_STG = os.path.join(os.path.dirname(__file__), "./sql/stg_load_users.sql")
LOAD_DIALOGS_TO_STG = os.path.join(os.path.dirname(__file__), "./sql/stg_load_dialogs.sql")
LOAD_GROUPS_TO_STG = os.path.join(os.path.dirname(__file__), "./sql/stg_load_groups.sql")


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
    tags=["sprint6", "get_data"],
)

def sprint6_get_data():
    
    @task
    def fetch_groups_csv():
        s3 = get_s3_client()
        s3.download_file(
            Bucket="sprint6",
            Key="groups.csv",
            Filename="/data/groups.csv"
        )
        return "/data/groups.csv"

    @task
    def fetch_users_csv():
        s3 = get_s3_client()
        s3.download_file(
            Bucket="sprint6",
            Key="users.csv",
            Filename="/data/users.csv"
        )
        return "/data/users.csv"

    @task
    def fetch_dialogs_csv():
        s3 = get_s3_client()
        s3.download_file(
            Bucket="sprint6",
            Key="dialogs.csv",
            Filename="/data/dialogs.csv"
        )
        return "/data/dialogs.csv"

    @task
    def print_head(files: list):
        for file_path in files:
            print("----- first 10 lines of:", file_path, "-----")
            with open(file_path, "r", encoding="utf-8") as f:
                for i in range(10):
                    line = f.readline()
                    if not line:
                        break
                    print(line.rstrip("\n"))
            print("--------------------------------------------")
    
    @task
    def create_vertica_tables(sql_file: str):
        hook = VerticaHook(vertica_conn_id="vertica_conn")

        with open(sql_file, "r") as f:
            query = f.read()
        hook.run(query)    

    @task
    def load_users_csv(sql_file: str):
        hook = VerticaHook(vertica_conn_id="vertica_conn")

        with open(sql_file, "r") as f:
            query = f.read()
        hook.run(query)    

    @task
    def load_dialogs_csv(sql_file: str):
        hook = VerticaHook(vertica_conn_id="vertica_conn")

        with open(sql_file, "r") as f:
            query = f.read()
        hook.run(query)    

    @task
    def load_groups_csv(sql_file: str):
        hook = VerticaHook(vertica_conn_id="vertica_conn")

        with open(sql_file, "r") as f:
            query = f.read()
        hook.run(query)    
        

    # ----- DAG graph -----
    groups = fetch_groups_csv()
    users = fetch_users_csv()
    dialogs = fetch_dialogs_csv()
    print_10_lines = print_head([groups, users, dialogs]) 
    create_tables_task = create_vertica_tables(SQL_FILE)

    load_groups_to_stg = load_groups_csv(LOAD_GROUPS_TO_STG) 
    load_users_to_stg = load_users_csv(LOAD_USERS_TO_STG)
    load_dialogs_to_stg = load_dialogs_csv(LOAD_DIALOGS_TO_STG)

    [groups, users, dialogs] >> print_10_lines >> create_tables_task >> [load_users_to_stg, load_dialogs_to_stg, load_groups_to_stg]

_ = sprint6_get_data()