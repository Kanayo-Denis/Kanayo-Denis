from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt 
#defining DAG arguments
default_args = {
    'owner': 'Menace',
    'start_date': days_ago(0),
    'email': ['dummy@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'ETL_toll_data',
    'schedule_interval=timedelta(days=1)',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
)

# task 1
task1 = BashOperator(
    task_id ='unzip_data',
    bash_command= ' tar --extract --file /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment',
    dag=dag,
)

# task 2
task2 = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -f1,2,3,4 -d"," /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv',
    dag=dag
)
# task 3
task3 = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5,6,7,8 -d$"\t" --output-delimiter="," /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv
    dag=dag
)

task4 = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -f6,7 --output-delimiter"," /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv,
    dag=dag 
)

task5 = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d"," /home/project/airflow/dags/finalassignment/csv_data.csv /home/project/airflow/dags/finalassignment/tsv_data.csv /home/project/airflow/dags/finalassignment/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/extracted_data.csv,
    dag=dag
)

task6 = BashOperator(
    task_id='transform_data',
    bash_command='tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv,
)


task1 >> task2 >> task3 >> task4 >> task5 >> task 6