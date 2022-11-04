
from distutils.ccompiler import new_compiler
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
import pandas as pd
import json
from pymongo import MongoClient
import re
from datetime import datetime


#Extract file from source location
def extract_data():
    url = 'https://drive.google.com/file/d/1NC_5yMQL6ch2CqdZa8j0jGcg714UESjW/view?usp=sharing'
    url = 'https://drive.google.com/uc?id=' + url.split('/')[-2]
    global df
    df = pd.read_csv(url)
    return "success"


#function to remove all unnessecary symbols
def remove_emoji(text):
    restricted_symbols = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"
        u"\U0001F300-\U0001F5FF"
        u"\U0001F680-\U0001F6FF"  
        u"\U0001F1E0-\U0001F1FF"  
        u"\U00002500-\U00002BEF"  
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"
        u"\u3030"
                      "]+", flags = re.UNICODE)
    return restricted_symbols.sub(r'',text)


#Transformin data
def transform_data():
    global df1
    df1 = df.copy()
    df1 = df1.dropna(how = 'all')
    df1 = df1.fillna('-')
    df1['at'] = df1['at'].replace('-', '2000-01-01 00:00:00')
    df1 = df1.sort_values(by = 'at', ascending = False)
    df1['content'] = df1['content'].apply(remove_emoji)


#Saving data
def save_data():
    df1.to_csv('/home/andrei/airflow/dags/transformed_data.csv', index = False, sep='|')


#Creating a new mongodb collection
def create_collection():
    conn = 'mongodb+srv://admin:admin@cluster0.7dvukyg.mongodb.net'
    client = MongoClient(conn)
    db = client['mongo-main']
    print('Connected to database')
    db.drop_collection('google_reviews')
    collection = db.create_collection('google_reviews')
    db.list_collection_names()
    data = pd.read_csv('/home/andrei/airflow/dags/transformed_data.csv', delimiter='|')
    payload = json.loads(data.to_json(orient='records'))
    collection.insert_many(payload)
    return collection.count_documents({})


def etl_process():
    extract_data()
    transform_data()
    save_data()


with DAG('mongodb_task', start_date = datetime(2022,10,27),
schedule_interval = '@once',
catchup = False) as dag:

    etl_step = PythonOperator(task_id = "etl_data", python_callable = etl_process)
    db_step = PythonOperator(task_id = "create_mongo_collection", python_callable = create_collection)

    etl_step >> db_step