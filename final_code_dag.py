from datetime import timedelta
2from airflow import DAG
3from airflow.operators.python import PythonOperator
4from airflow.utils.dates import days_ago
5import pandas as pd
6import requests
7import io
8import psycopg2
9import pendulum
10
11# Function
12
13def extract_weather_data():
14    key = "f6f4e999c7394519b35142300232111"
15    city = "Belo Horizonte"
16    URL = f"http://api.weatherapi.com/v1/forecast.json?key={key}&q={city}&days=7"
17
18    weather_data = requests.get(URL)
19    weather_json = weather_data.json()
20
21    # Manage the data into a dictionary
22    # We need to make some intermidiate dictionarys
23
24    City = weather_json["location"]["name"]
25
26    day = []
27    max_temp =[]
28    min_temp = []
29    avg_temp =[]
30    rain_mm = []
31    rain_chance = []
32
33
34    for date in weather_json["forecast"]["forecastday"]:
35        day.append(date["date"])
36
37    for i in weather_json["forecast"]["forecastday"]:
38        max_temp.append(i["day"]["maxtemp_c"])
39        min_temp.append(i["day"]["mintemp_c"])
40        avg_temp.append(i["day"]["avgtemp_c"])
41        rain_mm.append(i["day"]["totalprecip_mm"])
42        rain_chance.append(i["day"]["daily_chance_of_rain"])
43
44
45    final_dict = {
46        "city": City,
47        "max_temp": max_temp,
48        "min_temp": min_temp,
49        "avg_temp": avg_temp,
50        "rain_mm": rain_mm,
51        "rain_chance": rain_chance
52    }
53
54    df_weather = pd.DataFrame(final_dict)
55    return df_weather
56
57def load_data(df, tabela, colunas):
58    conn = psycopg2.connect(host='localhost', port='5432', database='postgres', user='admin', password='admin')
59    cur = conn.cursor()
60    output = io.StringIO()
61    df.to_csv(output, sep='\t', header = True, index = False)
62    output.seek(0)
63    try:
64        cur.copy_from(output, tabela, null = "", columns = colunas)
65        conn.commit()
66    except Exception as e:
67        print(e)
68        conn.rollback()
69
70# DAG
71
72default_args = {
73    'owner': 'airflow',
74    'depends_on_past': False,
75    'retries': 1,
76    'retry_delay': timedelta(minutes=1),
77    'start_date': pendulum.today('UTC').add(days=-1) 
78}
79
80dag = DAG(
81    'weather_api_data',
82    default_args=default_args,
83    description='Extract weather data from Weather API',
84    schedule='0 0 * * 0',
85)
86
87def extract_and_return_data(**kwargs):
88    extracted_data = extract_weather_data()
89    kwargs['ti'].xcom_push(key='extracted_data', value=extracted_data)
90
91extract_task = PythonOperator(
92    task_id='extract_weather_data',
93    python_callable=extract_and_return_data,
94    dag=dag, 
95)
96
97def load_data_from_xcom(**kwargs):
98    extracted_data = kwargs['ti'].xcom_pull(task_ids='extract_weather_data', key='extracted_data')
99    
100    load_data(extracted_data, 'weather_bh', ['city', 'max_temp',
101                                              'min_temp',
102                                              'avg_temp',
103                                              'rain_mm',
104                                              'rain_chance'])
105
106
107load_task = PythonOperator(
108    task_id='load_data',
109    python_callable=load_data_from_xcom,
110    dag=dag, 
111)
112
113# Set the task dependency
114extract_task >> load_task

