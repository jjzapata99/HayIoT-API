import math
import psycopg2 as psycopg2
import pytz
from bson import ObjectId
import datetime
import pandas as pd
from pydantic import BaseModel
from pymongo import MongoClient

class sensor(BaseModel):
    description: str
    siteRef: str
    equipRef: str
    type: str
    data: str
    sensedAt : str

postgresdb = psycopg2.connect(
    host="200.126.14.233",
    database="HayIoT",
    user="administrator",
    password="root1234")
postgres = postgresdb.cursor()
client = MongoClient('mongodb://200.126.14.233:27017/')
db= client['HayIoT']
c_sensor= db['sensor']
postgres.execute("SELECT * FROM site")
print(postgres.fetchall())
postgres.execute("SELECT * FROM equip")
print(postgres.fetchall())
postgres_insert_query3 = """ INSERT INTO sensor (id, siteRef, equipRef, type, description) VALUES (%s,%s,%s,%s,%s)"""
def validar_existencia(sended : sensor):
    postgres.execute("SELECT * FROM sensor WHERE siteref = %s AND equipref = %s AND type = %s AND description = %s",
                     (sended.siteRef, sended.equipRef, sended.type, sended.description))
    query =postgres.fetchall()
    if len(query) == 0:
        id_sensor= str(ObjectId())
        record_to_insert = (id_sensor, sended.siteRef, sended.equipRef, sended.type, sended.description)
        postgres.execute(postgres_insert_query3, record_to_insert)
        postgresdb.commit()
        return id_sensor
    else:
        return query[0][0]
def input_data(sensed : sensor):
    try:
        id_sensor = validar_existencia(sensed)
        c_sensor.insert_one({ "id_sensor": id_sensor, "description": sensed.description, "data": sensed.data, "type": sensed.type ,"sensedAt": datetime.datetime.now(pytz.utc) })
        postgres.execute("SELECT * FROM sensor")
        print(postgres.fetchall())
    except:
        print("Error al ingresar los datos")
def input_multiple_data(sensed : sensor):
    try:
        id_sensor = validar_existencia(sensed)
        for i in sensed.data:
            c_sensor.insert_one({ "id_sensor": id_sensor, "description": sensed.description, "data": sensed.data[i], "type": i ,"sensedAt": datetime.datetime.strptime(sensed.sensedAt, '%Y-%m-%dT%H:%M:%S') })
        postgres.execute("SELECT * FROM sensor")
        print(postgres.fetchall())
    except:
        print("No es un json")
def getSensors(id : str = '', name : str = '', max: int= 10, index: int = 0 ):
    try:
        if name != '' : name = '%'+name+'%'
        if id != '': id = '%'+ id + '%'
        postgres.execute("SELECT * FROM sensor WHERE id LIKE %s OR description LIKE %s", (id, name))
        query= postgres.fetchall()
        lista = []
        for i in query:
            lista.append({'id': i[0], 'siteref': i[1], 'equipref':i[2], 'type':i[3], 'description':i[4]})
        return {'data':lista[index*max: max*(index+1)],'indexs': list(range(math.ceil(len(lista)/max)))}
    except:
        return []
def getData(id,start, end):
    try:
        query= {'sensedAt':{'$gte': datetime.datetime.strptime(start, '%d/%m/%Y'), '$lt': datetime.datetime.strptime(end + " 23:59:59", '%d/%m/%Y %H:%M:%S')}, 'id_sensor': id}
        df_sensed = pd.DataFrame(list(c_sensor.find(query)))
        df_sensed['_id'] = df_sensed['_id'].astype('|S')
        df = df_sensed[["id_sensor","description","data","type","sensedAt"]].to_dict(orient='records')
        if df_sensed.size >= 1:
            return df

    except:
        return []