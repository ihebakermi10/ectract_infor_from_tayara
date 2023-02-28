
from airflow.operators.python import PythonOperator


import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from airflow import DAG
import datetime

current_datetime = datetime.datetime.now()
print(type(current_datetime))
def extract_title(url, **kwargs):
    response = requests.get(url)
    html_content = response.text
    soup = BeautifulSoup(html_content, "html.parser")
    streaming_data = soup.find_all("h2", {"class": "card-title font-arabic text-sm font-medium leading-5 text-gray-800 max-w-min min-w-full line-clamp-2 mb-2 mt-1"}) 

    print('iheb')
    data = []
    test=["IPHONE"]
    for i in streaming_data:
     for j in test : 
      if j in  i.text.upper()  :
      
       data.append(i.text)
    
    return(data)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':current_datetime,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'extract_title_dag',
    default_args=default_args,
    description='Extracts DATA FROM website',
    schedule='0 0 * * *',
    catchup=False
)


extract_title_task = PythonOperator(
    task_id='extract_title',
    python_callable=extract_title,
    op_args=['https://www.tayara.tn/search/?category=Informatique+et+Multimedia'],
    dag=dag)

def end_workflow():
    print("Workflow completed successfully!")

end_task = PythonOperator(
    task_id='end_task',
    python_callable=end_workflow,
    dag=dag
)



extract_title_task >> end_task