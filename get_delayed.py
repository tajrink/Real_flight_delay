import psycopg2
import pandas as pd
import numpy as np

params = {
    "host"      : "localhost",
    "dbname"    : "Flights",
    "user"      : "postgres",
    "password"  : "Tultul20",
    "port" : "5432"     
}

conn = psycopg2.connect(**params)

cursor = conn.cursor()
cursor.execute("SELECT * FROM real_flight WHERE cancelled != '1' and diverted !='1'")
rows = cursor.fetchall()
#print(rows)

cursor.close()

df = pd.DataFrame(rows, columns=[desc.name for desc in cursor.description])
print(df.head())

print(df.isnull().sum())

df['DELAYED'] = np.where(((df["dep_del15"] == '1') | (df["arr_del15"] == '1')), 1, 0)

gr_air=df.groupby("op_unique_carrier").mean()["DELAYED"]
sorted_air=gr_air.sort_values(ascending=False)
sorted_air.to_csv('delayed_airlines.csv')


gr_origin=df.groupby("origin").mean()['DELAYED']
sorted_air=gr_origin.sort_values(ascending=False)
sorted_air=gr_origin.to_csv('delayed_airports.csv')
