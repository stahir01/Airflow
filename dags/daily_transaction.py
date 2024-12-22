import os
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pandas as pd
from google.cloud import bigquery

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['alimurad7777@gmail.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_transaction',
    default_args=default_args,
    description='ETL pipeline to extract and load daily transaction data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)


def debug_path(file_path):
    print(f"Current working directory: {os.getcwd()}")
    full_path = os.path.join(os.getcwd(), file_path)
    print(f"Resolved full path: {full_path}")
    return full_path

def get_data_from_csv(file_path):
    resolved_path = debug_path(file_path)
    if not os.path.exists(resolved_path):
        raise FileNotFoundError(f"File not found at: {resolved_path}")
    return pd.read_csv(resolved_path)

def extract_transactions(**kwargs):
    file_path = "dataset/Transactions_Data.csv"  # Relative path to your CSV file
    df = get_data_from_csv(file_path)
    return df.to_dict()

def extract_users(**kwargs):
    file_path = "dataset/Users_Data.csv"  # Relative path to your CSV file
    df = get_data_from_csv(file_path)
    return df.to_dict()

def extract_user_preferences(**kwargs):
    file_path = "dataset/User_Preferences_Data.csv"  # Relative path to your CSV file
    df = get_data_from_csv(file_path)
    return df.to_dict()

def create_usertransactions_df(**kwargs):
    # Fetch data from XCom
    transactions_dict = kwargs['ti'].xcom_pull(task_ids='extract_transactions')
    users_dict = kwargs['ti'].xcom_pull(task_ids='extract_user_data')
    preferences_dict = kwargs['ti'].xcom_pull(task_ids='extract_user_preferences')

    # Create DataFrames from the dictionaries
    transactions_df = pd.DataFrame.from_dict(transactions_dict)
    users_df = pd.DataFrame.from_dict(users_dict)
    preferences_df = pd.DataFrame.from_dict(preferences_dict)

    # Rename columns
    transactions_df.rename(columns={'id': 'transaction_id', 'amount': 'transaction_amount', 'type': 'transaction_type', }, inplace=True)
    users_df.rename(columns={'id': 'user_id', 'name': 'user_name'}, inplace=True)
    preferences_df.rename(columns={'id': 'preference_id', 'created_at': 'preference_created_at', 'updated_at': 'preference_updated_at'}, inplace=True)

    # Perform merges
    combined_df = transactions_df.merge(users_df, left_on='user_id', right_on='user_id', how='inner')
    combined_df = combined_df.merge(preferences_df, left_on='user_id', right_on='user_id', how='inner')

    # Select necessary columns for combined DataFrame
    combined_df = combined_df[
        [
            'transaction_date', 'user_id', 'user_name', 
            'registration_date', 'email', 'transaction_amount', 'transaction_type', 
            'preferred_language', 'notifications_enabled', 
            'marketing_opt_in', 'preference_created_at', 'preference_updated_at'
        ]
    ]

    print(f"Combined Data Frame Preview: \n {combined_df.head()}")

    return combined_df.to_dict()

def convert_clean_data(**kwargs):
    # Fetch the combined DataFrame from XCom
    combined_dict = kwargs['ti'].xcom_pull(task_ids='create_usertransactions_df')
    combined_df = pd.DataFrame.from_dict(combined_dict)

    # Convert and clean data
    try:
        combined_df['transaction_date'] = pd.to_datetime(combined_df['transaction_date'], errors='coerce').dt.date
        combined_df['user_id'] = pd.to_numeric(combined_df['user_id'], errors='coerce').fillna(0).astype(int)
        combined_df['user_name'] = combined_df['user_name'].astype(str)
        combined_df['registration_date'] = pd.to_datetime(combined_df['registration_date'], errors='coerce').dt.date
        combined_df['email'] = combined_df['email'].astype(str)
        combined_df['transaction_amount'] = pd.to_numeric(combined_df['transaction_amount'], errors='coerce')
        combined_df['transaction_type'] = combined_df['transaction_type'].astype(str)
        combined_df['preferred_language'] = combined_df['preferred_language'].astype(str)
        combined_df['notifications_enabled'] = combined_df['notifications_enabled'].astype(bool)
        combined_df['marketing_opt_in'] = combined_df['marketing_opt_in'].astype(bool)
        combined_df['preference_created_at'] = pd.to_datetime(combined_df['preference_created_at'], errors='coerce')
        combined_df['preference_updated_at'] = pd.to_datetime(combined_df['preference_updated_at'], errors='coerce')
    except Exception as e:
        raise ValueError(f"Error converting columns: {e}")

    # Convert date columns to string
    date_columns = ['transaction_date', 'registration_date', 'preference_created_at', 'preference_updated_at']
    for col in date_columns:
        combined_df[col] = combined_df[col].astype(str)

    print(f"Cleaned Data Frame Preview: \n {combined_df.head()}")

    return combined_df.to_dict()


BQ_CONN_ID = 'gfc_conn_key_new'
BQ_PROJECT_ID = 'concise-rampart-445016-f0'
BQ_DATASET = 'Ancient_Gaming_Dataset'

def generate_insert_query(**kwargs):
    clean_data = kwargs['ti'].xcom_pull(task_ids='convert_clean_data')
    clean_df = pd.DataFrame.from_dict(clean_data)


    values = ", ".join([
        f"('{row.transaction_date}', {row.user_id}, '{row.user_name}', '{row.registration_date}', '{row.email}', "
        f"{row.transaction_amount}, '{row.transaction_type}', '{row.preferred_language}', {row.notifications_enabled}, "
        f"{row.marketing_opt_in}, '{row.preference_created_at}', '{row.preference_updated_at}')"
        for _, row in clean_df.iterrows()
    ])

    # Construct the full INSERT query
    query = f"""
    INSERT INTO `{BQ_PROJECT_ID}.{BQ_DATASET}.User_Transactions_Preferences`
    (transaction_date, user_id, user_name, registration_date, email, 
     transaction_amount, transaction_type, preferred_language, notifications_enabled, 
     marketing_opt_in, preference_created_at, preference_updated_at)
    VALUES {values};
    """
    return query

extract_transactions_data = PythonOperator(
    task_id='extract_transactions',
    provide_context=True,
    python_callable=extract_transactions,
    email=None,
    dag=dag,
    )

extract_user_data = PythonOperator(
    task_id='extract_user_data',
    python_callable=extract_users,
    provide_context=True,
    email=None,
    dag=dag,
    )


extract_user_preferences_data= PythonOperator(
    task_id='extract_user_preferences',
    python_callable=extract_user_preferences,
    provide_context=True,
    email=None,
    dag=dag,
    )

combined_transactions_data = PythonOperator(
    task_id='create_usertransactions_df',
    python_callable=create_usertransactions_df,
    provide_context=True,
    email=None,
    dag=dag,
    )


convert_clean_df = PythonOperator(
    task_id='convert_clean_data',
    python_callable=convert_clean_data,
    provide_context=True,
    email=None,
    dag=dag,
    )

generate_query = PythonOperator(
    task_id='generate_insert_query',
    python_callable=generate_insert_query,
    provide_context=True,
    email=None,
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





[extract_transactions_data, extract_user_data, extract_user_preferences_data] >> combined_transactions_data >> convert_clean_df >> generate_query >> insert_data_to_bigquery





