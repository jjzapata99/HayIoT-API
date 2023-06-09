import psycopg2 as psycopg2
import pytz
from bson import ObjectId
import datetime
import pandas as pd
from pydantic import BaseModel
from pymongo import MongoClient
import requests

class sensor(BaseModel):
    siteRef: str
    equipRef: str
    unit: str
    data: str
#some_info = {'info':'some_info'}
#jsondata= { "siteRef": "string","equipRef": "string", "unit": "string", "data": "string"}
#head = 'http://localhost:8000'
#requests.post(f'{head}/items/', json=jsondata)
postgresdb = psycopg2.connect(
    host="200.126.14.233",
    database="HayIoT",
    user="administrator",
    password="root1234")
postgres = postgresdb.cursor()
client = MongoClient('mongodb://200.126.14.233:27017/')
db= client['HayIoT']
c_sensor= db['sensor']
##### Iniciar papaya
#postgres_insert_query1 = """ INSERT INTO site (id, site) VALUES (%s,%s)"""
#postgres.execute(postgres_insert_query1, ("C11","Laboratorios Fiec"))
#postgresdb.commit()
#postgres_insert_query2 = """ INSERT INTO equip (id, siteRef, equip) VALUES (%s,%s,%s)"""
#postgres.execute(postgres_insert_query2, ("LT1","C11", "Papaya"))
#postgresdb.commit()
#################
postgres.execute("SELECT * FROM site")
print(postgres.fetchall())
postgres.execute("SELECT * FROM equip")
print(postgres.fetchall())
#postgres.execute("SELECT * FROM sensor WHERE siteref = %s AND equipref = %s AND unit = %s", ("C11", "LT1", "celcius"))
#print(len(postgres.fetchall()))
postgres_insert_query3 = """ INSERT INTO sensor (id, siteRef, equipRef, unit) VALUES (%s,%s,%s,%s)"""
def validar_existencia(sended : sensor):
    postgres.execute("SELECT * FROM sensor WHERE siteref = %s AND equipref = %s AND unit = %s", (sended.siteRef, sended.equipRef, sended.unit))
    query =postgres.fetchall()
    if len(query) == 0:
        id_sensor= str(ObjectId())
        record_to_insert = (id_sensor, sended.siteRef, sended.equipRef, sended.unit)
        postgres.execute(postgres_insert_query3, record_to_insert)
        postgresdb.commit()
        return id_sensor
    else:
        return query[0][0]

def input_data(sensed : sensor):
    id_sensor = validar_existencia(sensed)
    c_sensor.insert_one({ "id_sensor": id_sensor, "data": sensed.data, "unit": sensed.unit ,"sensedAt": datetime.datetime.now(pytz.utc) })
    postgres.execute("SELECT * FROM sensor")
    print(postgres.fetchall())
#print(conn.cursor().execute("INSERT INTO site (id, site) VALUES ('asd', 'asd')"))



