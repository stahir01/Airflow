import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Environment Variables
EMAIL_ADDRESS = os.getenv('EMAIL_ADDRESS')
BQ_CONN_ID = os.getenv('BQ_CONN_ID')
BQ_PROJECT_ID = os.getenv('BQ_PROJECT_ID')
BQ_DATASET = os.getenv('BQ_DATASET')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': [EMAIL_ADDRESS],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'daily_transaction',
    default_args=default_args,
    description='ETL pipeline to extract, clean, and load daily transaction data into BigQuery',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# Utility Functions
def debug_path(file_path: str) -> str:
    """
    Debug and resolve the full file path for a given relative path.
    """

    full_path = os.path.join(os.getcwd(), file_path)
    print(f"Resolved full path: {full_path}")
    return full_path

def get_data_from_csv(file_path: str) -> pd.DataFrame:
    """
    Read a CSV file into a Pandas DataFrame.
    """

    resolved_path = debug_path(file_path)
    if not os.path.exists(resolved_path):
        raise FileNotFoundError(f"File not found at: {resolved_path}")
    return pd.read_csv(resolved_path)

# Data Extraction Functions
def extract_transactions(**kwargs) -> dict:
    file_path = "dataset/Transactions_Data.csv"
    df = get_data_from_csv(file_path)
    return df.to_dict()

def extract_users(**kwargs) -> dict:
    file_path = "dataset/Users_Data.csv"
    df = get_data_from_csv(file_path)
    return df.to_dict()

def extract_user_preferences(**kwargs) -> dict:
    file_path = "dataset/User_Preferences_Data.csv"
    df = get_data_from_csv(file_path)
    return df.to_dict()

# Data Transformation Functions
def create_usertransactions_df(**kwargs) -> dict:
    """
    Merge and transform transactions, users, and preferences data into a single DataFrame.

    Returns:
        dict: Transformed data as a dictionary.
    """
    transactions_dict = kwargs['ti'].xcom_pull(task_ids='extract_transactions')
    users_dict = kwargs['ti'].xcom_pull(task_ids='extract_user_data')
    preferences_dict = kwargs['ti'].xcom_pull(task_ids='extract_user_preferences')

    transactions_df = pd.DataFrame.from_dict(transactions_dict)
    users_df = pd.DataFrame.from_dict(users_dict)
    preferences_df = pd.DataFrame.from_dict(preferences_dict)

    transactions_df.rename(columns={'id': 'transaction_id', 'amount': 'transaction_amount', 'type': 'transaction_type'}, inplace=True)
    users_df.rename(columns={'id': 'user_id', 'name': 'user_name'}, inplace=True)
    preferences_df.rename(columns={'id': 'preference_id', 'created_at': 'preference_created_at', 'updated_at': 'preference_updated_at'}, inplace=True)

    combined_df = transactions_df.merge(users_df, on='user_id').merge(preferences_df, on='user_id')

    selected_columns = [
        'transaction_date', 'user_id', 'user_name', 'registration_date', 'email', 
        'transaction_amount', 'transaction_type', 'preferred_language', 
        'notifications_enabled', 'marketing_opt_in', 'preference_created_at', 
        'preference_updated_at'
    ]
    combined_df = combined_df[selected_columns]

    print(f"Combined Data Frame Preview: \n{combined_df.head()}")
    return combined_df.to_dict()

def generate_insert_query(**kwargs) -> str:
    """
    Generate a SQL INSERT query from the cleaned data.

    Returns:
        str: SQL INSERT query string.
    """
    clean_data = kwargs['ti'].xcom_pull(task_ids='create_usertransactions_df')
    clean_df = pd.DataFrame.from_dict(clean_data)

    values = ", ".join([
        f"('{row.transaction_date}', {row.user_id}, '{row.user_name}', '{row.registration_date}', '{row.email}', "
        f"{row.transaction_amount}, '{row.transaction_type}', '{row.preferred_language}', {row.notifications_enabled}, "
        f"{row.marketing_opt_in}, '{row.preference_created_at}', '{row.preference_updated_at}')"
        for _, row in clean_df.iterrows()
    ])

    query = f"""
    INSERT INTO `{BQ_PROJECT_ID}.{BQ_DATASET}.User_Transactions_Preferences`
    (transaction_date, user_id, user_name, registration_date, email, 
     transaction_amount, transaction_type, preferred_language, notifications_enabled, 
     marketing_opt_in, preference_created_at, preference_updated_at)
    VALUES {values};
    """
    return query

# DAG Tasks
extract_transactions_data = PythonOperator(
    task_id='extract_transactions',
    python_callable=extract_transactions,
    dag=dag,
)

extract_user_data = PythonOperator(
    task_id='extract_user_data',
    python_callable=extract_users,
    dag=dag,
)

extract_user_preferences_data = PythonOperator(
    task_id='extract_user_preferences',
    python_callable=extract_user_preferences,
    dag=dag,
)

combined_transactions_data = PythonOperator(
    task_id='create_usertransactions_df',
    python_callable=create_usertransactions_df,
    dag=dag,
)

generate_query = PythonOperator(
    task_id='generate_insert_query',
    python_callable=generate_insert_query,
    dag=dag,
)

insert_data_to_bigquery = BigQueryInsertJobOperator(
    task_id='insert_data_to_bigquery',
    configuration={
        "query": {
            "query": "{{ task_instance.xcom_pull(task_ids='generate_insert_query') }}",
            "useLegacySql": False
        }
    },
    gcp_conn_id=BQ_CONN_ID,
    location='US',
    dag=dag,
)

# DAG Task Dependencies
[extract_transactions_data, extract_user_data, extract_user_preferences_data] >> combined_transactions_data >> generate_query >> insert_data_to_bigquery
