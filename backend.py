import math
import psycopg2 as psycopg2
import pymongo
import pytz
from bson import ObjectId
import datetime
import pandas as pd
from pydantic import BaseModel
from pymongo import MongoClient
import requests


url = "https://project-haystack.org/download/defs.json"

class dataModel(BaseModel):
    val: float
    type: str


class sensor(BaseModel):
    siteRef: str
    equipRef: str
    description: str
    type: str


class sensors(BaseModel):
    id: str
    data: list[dataModel]
    sensedAt: str


class equipo(BaseModel):
    id: str
    siteRef: str
    equip: str


class site(BaseModel):
    id: str
    site: str


postgresdb = psycopg2.connect(
    host="200.126.14.233",
    database="HayIoT",
    user="administrator",
    password="root1234")
postgres = postgresdb.cursor()
client = MongoClient('mongodb://200.126.14.233:27017/')
db = client['HayIoT']
c_sensor = db['sensor']
postgres_insert_query = """INSERT INTO site (id, site) VALUES (%s,%s)"""
postgres_insert_query2 = """INSERT INTO equip (id, siteRef, equip) VALUES (%s,%s,%s)"""
postgres_insert_query3 = """ INSERT INTO sensor (id, siteRef, equipRef, type, description) VALUES (%s,%s,%s,%s,%s)"""


def validar_existencia(sended: sensors):
    try:
        postgres.execute("SELECT * FROM sensor WHERE LOWER(id) = %s", (sended.id.lower(),))
        query = postgres.fetchall()
        if len(query) > 0:
            return 1
        else:
            return 'asd'
    except Exception as e:
        print(f'{e}')

# def input_data(sensed : sensor):
#    try:
#        id_sensor = validar_existencia(sensed)
#        c_sensor.insert_one({ "id_sensor": id_sensor, "description": sensed.description, "data": sensed.data, "type": sensed.type ,"sensedAt": datetime.datetime.now(pytz.utc) })
#        postgres.execute("SELECT * FROM sensor")
#        print(postgres.fetchall())
#    except:
#        print("Error al ingresar los datos")
#
def input_multiple_data(sensed: sensors):
    try:
        if (validar_existencia(sensed)):
            d = sensed.sensedAt
            if (d == ''):
                d = datetime.datetime.now(pytz.utc)
            else:
                d = datetime.datetime.strptime(sensed.sensedAt, '%Y-%m-%dT%H:%M:%S')
            for i in sensed.data:
                c_sensor.insert_one({"id_sensor": sensed.id, "data": i.val, "type": i.type, "sensedAt": d})
        return 1
    except Exception as e:
        print(f'{e}')
        print('Error al ingresar el sensado')
        return 0


def getSensors(id: str = '', name: str = '', max: int = 10, index: int = 0):
    try:
        if name != '': name = '%' + name.lower() + '%'
        if id != '': id = '%' + id.lower() + '%'
        postgres.execute("SELECT * FROM sensor WHERE LOWER(id) LIKE %s OR LOWER(description) LIKE %s", (id, name))
        query = postgres.fetchall()
        lista = []
        for i in query:
            lista.append({'id': i[0], 'siteref': i[1], 'equipref': i[2], 'type': i[3], 'description': i[4]})
        return {'data': lista[index * max: max * (index + 1)], 'indexs': list(range(math.ceil(len(lista) / max)))}
    except Exception as e:
        print(f'{e}')
        return []

def getLastDate(id : str):
    try:
        q=pd.DataFrame(list( c_sensor.find({'id_sensor':id},{'sensedAt':1}).sort('sensedAt', pymongo.DESCENDING).limit(1)))
        if(len(q)>0): q =q['sensedAt'][0]
        else: q = 'Nan'
        return {'lastSensed':q}
    except Exception as e:
        print(f'{e}')


def getData(id, start, end):
    try:
        query = {'sensedAt': {'$gte': datetime.datetime.strptime(start, '%d/%m/%Y'),
                              '$lt': datetime.datetime.strptime(end + " 23:59:59", '%d/%m/%Y %H:%M:%S')},
                 'id_sensor': id}
        df_sensed = pd.DataFrame(list(c_sensor.find(query)))
        df_sensed['_id'] = df_sensed['_id'].astype('|S')
        df = df_sensed[["id_sensor", "data", "type", "sensedAt"]].to_dict(orient='records')
        if df_sensed.size >= 1:
            return df

    except Exception as e:
        print(f'{e}')
        return []


def input_page_data_sensor(s: sensor):
    try:
        postgres.execute(
            "SELECT * FROM sensor WHERE LOWER(siteref) = %s AND LOWER(equipref) = %s AND LOWER(type) = %s AND LOWER(description) = %s",
            (s.siteRef.lower(), s.equipRef.lower(), s.type.lower(), s.description.lower()))
        query = postgres.fetchall()
        if len(query) == 0:
            id_sensor = str(ObjectId())
            record_to_insert = (id_sensor, s.siteRef, s.equipRef, s.type, s.description)
            postgres.execute(postgres_insert_query3, record_to_insert)
            postgresdb.commit()
            return id_sensor
        else:
            return query[0][0]
    except Exception as e:
        print(f'{e}')
        return 0


def input_page_equip(equip: equipo):
    try:
        postgres.execute("SELECT * FROM equip WHERE LOWER(id) = %s AND LOWER(siteRef) = %s AND LOWER(equip) = %s",
                         (equip.id.lower(), equip.siteRef.lower(), equip.equip.lower()))
        query = postgres.fetchall()
        if len(query) == 0:
            record_to_insert = (equip.id, equip.siteRef, equip.equip)
            postgres.execute(postgres_insert_query2, record_to_insert)
            postgresdb.commit()
            return 1
        else:
            return 0
    except Exception as e:
        postgresdb.rollback()
        print(f'Erro al ingresar el equipo: {e}')
        return 0


def input_page_site(sit: site):
    try:
        postgres.execute("SELECT * FROM site WHERE LOWER(id) = %s AND LOWER(site) = %s",
                         (sit.id.lower(), sit.site.lower()))
        query = postgres.fetchall()
        if len(query) == 0:
            record_to_insert = (sit.id, sit.site)
            postgres.execute(postgres_insert_query, record_to_insert)
            postgresdb.commit()
            return 1
        else:
            return 0
    except Exception as e:
        print(f'{e}')
        postgresdb.rollback()
        print('Erro al ingresar el sitio')
        return 0


def getSites():
    try:
        postgres.execute("SELECT * FROM site")
        q = postgres.fetchall()
        return ({'id': x, 'site': y} for x, y in q)
    except Exception as e:
        print(f'{e}')
        return [{'id': '', 'site': ''}]

def getAllSensors():
    try:
        postgres.execute("SELECT * FROM sensor")
        q = postgres.fetchall()
        return ({'id': x, 'siteref': y, 'equipref': z, 'type': w, 'description': v} for x, y, z, w, v in q)
    except Exception as e:
        print(f'{e}')
        return [{'id': '', 'siteref': '', 'equipref': '', 'type': '', 'description': ''}]


def getEquips():
    try:
        postgres.execute("SELECT * FROM equip")
        q = postgres.fetchall()
        return ({'id': x, 'siteRef': y, 'equip': z} for x, y, z in q)
    except Exception as e:
        print(f'{e}')
        return [{'id': '', 'siteRef': '', 'equip': ''}]
    

def get_Haystack_tags():
    try:
        response = requests.get(url)
        if response.status_code == 200:
            datos= response.json()
            tags = []
            for obj in datos['rows']:
                if obj:
                    json_data = {
                        "tag": obj['def']['val'],
                        "description": obj['doc'].replace('\n', ' ')
                    }
                    tags.append(json_data)
            return tags
        else:
            print(f"Error al hacer la solicitud GET. Código de estado: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Error al hacer la solicitud GET: {e}")