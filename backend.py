import json
import math
from http.client import HTTPException
from typing import Union
from statistics import mode
import psycopg2 as psycopg2
import pymongo
import pytz
from bson import ObjectId
import datetime
import pandas as pd
from pydantic import BaseModel
from pymongo import MongoClient
import requests
from bson.json_util import dumps
url = "https://project-haystack.org/download/defs.json"

class dataModel(BaseModel):
    val: float
    type: str
class sensorPage(BaseModel):
    id: str
    siteRef: str
    equipRef: str
    description: str
    type: str
class sensor(BaseModel):
    siteRef: str
    equipRef: str
    description: str
    type: str
    tag: list[int]

class dataLast(BaseModel):
    id: str
    tags: list[str]
class sensors(BaseModel):
    id: str
    data: list[dataModel]
    sensedAt: Union[str, None]


class equipo(BaseModel):
    id: str
    siteRef: str
    equip: str


class site(BaseModel):
    id: str
    site: str

class dataWeb(BaseModel):
    id: str
    start: str
    end: str
    tags: Union[list[str], None]
try:
    postgresdb = psycopg2.connect(
        host="200.126.14.228",
        database="HayIoT",
        user="administrator",
        port= 3309,
        password="root1234")
    postgres = postgresdb.cursor()

except:
    print('Error al conectarse a la base de datos')
client = MongoClient('mongodb://200.126.14.228:3310/')

tages = []
tagList=["Power", "ApparentPower", "ReactivePower", "Factor", "Voltage", "Current",
         "humedad", "temperatura", "velocidad", "estado","temp_aire",
         "humedity", "temperature",
         "Valvula", "Humedad",
         "voltaje_B","voltaje_A","voltaje_C", "energia_C", "energia_B", "energia_A", "factor_potencia_C", "factor_potencia_A", "factor_potencia_B", "potencia_C","potencia_B","potencia_A", "corriente_C","corriente_A","corriente_B",
         "Humedad",
         "potencia_1","potencia_2","potencia_3", "FP_1","FP_2","FP_3",  "frecuencia", "value", "voltaje",  "corriente_1", "corriente_2", "corriente_3"
         ]
devices = [{'d':['648ca81672114d2f0457eb89', '648ca97e72114d2f0457ebaf', '648ca98972114d2f0457ebbf', '64b5cb98cca9b534b74527c0',
        '64b5cbfecca9b534b74528ed',
        '64b5cc16cca9b534b7452948', '64b5cc2dcca9b534b7452985', '64b5cc45cca9b534b74529c2', '64ee4675b6877e78eeea6545',
        '64ee468f23451b499bfb796a',
        '64ee46aab6877e78eeea65fd', '64ee46c8b6877e78eeea664b', '64ee46e4b6877e78eeea6697', '64ee46fb65540448be7e7d48',
        '64ee47143793664c6df9e86f',
        '64ee4728b6877e78eeea670a', '64ee46563793664c6df9e6d1', '648ca96a72114d2f0457eba0', '648ca97f72114d2f0457ebb7'], 'tags':["Power", "ApparentPower", "ReactivePower", "Factor", "Voltage", "Current"] },
           {'d':['64ac3680dc5442c4e078a0f9'], 'tags': ["humedad", "temperatura", "velocidad", "estado", "temp_aire"]},
           {'d':['64d30423e60fb2d1e556ae12'], 'tags':["Valvula", "Humedad"]},
           {'d':['64a474789f3db7cd3e9b53f7'],'tags':["humedity", "temperature"]},
           {'d':['64ad81a0dc5442c4e0796382','64ac62fcdc5442c4e078bb14', '658b00dbeaaab7bc755110ec', '658b0195df926417e987e76b'],'tags':["voltaje_B","voltaje_A","voltaje_C", "energia_C", "energia_B", "energia_A", "factor_potencia_C", "factor_potencia_A", "factor_potencia_B", "potencia_C","potencia_B","potencia_A", "corriente_C","corriente_A","corriente_B"]},
           {'d':['64d138d84659308ea6bc00b8'], 'tags':["potencia_1","potencia_2","potencia_3", "FP_1","FP_2","FP_3",  "frecuencia", "value", "voltaje",  "corriente_1", "corriente_2", "corriente_3"]}]



db = client['HayIoT']
c_sensor = db['sensor']
postgres_insert_query = """INSERT INTO site (id, site) VALUES (%s,%s)"""
postgres_insert_query2 = """INSERT INTO equip (id, siteRef, equip) VALUES (%s,%s,%s)"""
postgres_insert_query3 = """ INSERT INTO sensor (id, siteRef, equipRef, type, description) VALUES (%s,%s,%s,%s,%s)"""
postgres_insert_query4 = """ INSERT INTO sensor_tag (tag_id, sensor_id) VALUES (%s,%s)"""

def validar_tags(tags: list[dataModel]):
    for i in tags:
        if i.type not in tagList:
            return False
    return True
def validar_existencia(sended: sensors):
    try:
        postgres.execute("SELECT * FROM sensor WHERE LOWER(id) = %s", (sended.id.lower(),))
        query = postgres.fetchall()
        if len(query) > 0 and validar_tags(sended.data):
            return 1
        else:
            return 'asd'
    except Exception as e:
        postgresdb.rollback()
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
            if (d == '' or d is None):
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
        postgres.execute("SELECT * FROM sensor WHERE LOWER(id) LIKE %s OR LOWER(description) LIKE %s ORDER BY description DESC", (id, name))
        query = postgres.fetchall()
        lista = []
        for i in query:
            lista.append({'id': i[0], 'siteref': i[1], 'equipref': i[2], 'type': i[3], 'description': i[4]})
        return {'data': lista[index * max: max * (index + 1)], 'indexs': list(range(math.ceil(len(lista) / max)))}
    except Exception as e:
        postgresdb.rollback()
        print(f'{e}')
        return []

def getLastDate(id : str):
    try:
        q=pd.DataFrame(list( c_sensor.find({'id_sensor':id},{'sensedAt':1}).sort('sensedAt', pymongo.DESCENDING).limit(1)))
        tags= []
        if(len(q)>0): q =q['sensedAt'][0]
        else: q = 'Nan'
        if q != 'Nan':
            postgres.execute("SELECT tag_id FROM sensor_tag WHERE LOWER(sensor_id) LIKE %s ", (id, ))
            query = postgres.fetchall()
            if len(query) >0:
                for i in query:
                    postgres.execute("SELECT * FROM tag WHERE tag_id = %s ", (i[0], ))
                    qe = postgres.fetchall()
                    tags.append({'id':qe[0][0],"tag":qe[0][1]})
            else:
                for n in devices:
                    if id in n['d']:
                        c=1
                        for x in n['tags']:
                            tags.append({'id': c+1, 'tag':x})
                            c+=1
        return {'lastSensed':q, 'tags': tags}
    except Exception as e:
        postgresdb.rollback()
        print(f'{e}')


def getDataWeb(data: dataWeb):
    try:
        format = '%d/%m/%Y'
        if(len(data.start)>10):
            format= format+' %H:%M:%S'
        if not(len(data.end) > 10):
            data.end = data.end +" 23:59:59"
        if len(data.tags)==0:
            query = {'sensedAt': {'$gte': datetime.datetime.strptime(data.start, format),
                              '$lt': datetime.datetime.strptime(data.end, '%d/%m/%Y %H:%M:%S')},
                 'id_sensor': data.id}
        else:
            query = {'sensedAt': {'$gte': datetime.datetime.strptime(data.start, format),
                                  '$lt': datetime.datetime.strptime(data.end, '%d/%m/%Y %H:%M:%S')},
                     'id_sensor': data.id, 'type': {"$in": data.tags}}
        # print(list(c_sensor.find(query, {"_id":0, "data":1,  "sensedAt":1})))
        df_sensed = pd.DataFrame(list(c_sensor.find(query, {"_id":0, "data":1, "type":1, "sensedAt":1})))
        if(df_sensed.size>2):
            val = df_sensed['sensedAt'].max()- df_sensed['sensedAt'].min()
            if (val.days > 0):
                time = str((val.days)*4)+'T'
            else:
                d = df_sensed['sensedAt'].drop_duplicates().reset_index()
                fechas = [pd.to_datetime(fecha, format='%Y-%m-%d %H:%M:%S') for fecha in d['sensedAt']]
                diferencias_segundos = [(fechas[i+1] - fechas[i]).total_seconds() for i in range(len(fechas)-1)]
                contador = mode(diferencias_segundos)
                if len(d) < 18:
                    time = str(int(contador))+'s'
                else:
                    time = str(int(contador *10.5))+'s'
            df_sensed =df_sensed.set_index('sensedAt').groupby('type').resample(time).mean(numeric_only=True)
            df_sensed['data']=df_sensed['data'].fillna(0)
        df = df_sensed.sort_values('sensedAt').reset_index().to_json(orient='records')
        if df_sensed.size >= 1:
            return df
    except Exception as e:
        print(f'{e}')
        return json.dumps([])

def getData(id, start, end):
    try:
        format = '%d/%m/%Y'
        if(len(start)>10):
            format= format+' %H:%M:%S'
        if not(len(end) > 10):
            end = end +" 23:59:59"
        query = {'sensedAt': {'$gte': datetime.datetime.strptime(start, format),
                              '$lt': datetime.datetime.strptime(end, '%d/%m/%Y %H:%M:%S')},
                 'id_sensor': id}
        df_sensed = pd.DataFrame(list(c_sensor.find(query, {"_id":0, "data":1, "type":1, "sensedAt":1})))
        df = df_sensed.to_json(orient='records', date_format='iso', date_unit='s')
        if df_sensed.size >= 1:
            return df
    except Exception as e:
        print(f'{e}')
        return json.dumps([])

def getSpecificData(id, start, end, type):
    try:
        format = '%d/%m/%Y'
        if(len(start)>10):
            format= format+' %H:%M:%S'
        if not(len(end) > 10):
            end = end +" 23:59:59"
        query = {'sensedAt': {'$gte': datetime.datetime.strptime(start, format),
                              '$lt': datetime.datetime.strptime(end, '%d/%m/%Y %H:%M:%S')},
                 'id_sensor': id, 'type':type}
        df_sensed = pd.DataFrame(list(c_sensor.find(query, {"_id":0, "data":1, "type":1, "sensedAt":1})))
        df = df_sensed.to_json(orient='records', date_format='iso', date_unit='s')
        if df_sensed.size >= 1:
            return df
    except Exception as e:
        print(f'{e}')
        return json.dumps([])

def input_page_data_sensor(s: sensor):
    try:
        # postgres.execute("SELECT * FROM sensor WHERE LOWER(siteref) = %s AND LOWER(equipref) = %s AND LOWER(type) = %s AND LOWER(description) = %s",
        #     (s.siteRef.lower(), s.equipRef.lower(), s.type.lower(), s.description.lower()))
        postgres.execute("SELECT * FROM sensor WHERE LOWER(siteref) = %s AND LOWER(equipref) = %s AND LOWER(description) = %s",
                         (s.siteRef.lower(), s.equipRef.lower(), s.description.lower()))
        query = postgres.fetchall()
        if len(query) == 0:
            id_sensor = str(ObjectId())
            record_to_insert = (id_sensor, s.siteRef, s.equipRef, s.type, s.description)
            postgres.execute(postgres_insert_query3, record_to_insert)
            postgresdb.commit()
            for tag in s.tag:
                postgres.execute(postgres_insert_query4, (tag, id_sensor))
                postgresdb.commit()
            return {'id':id_sensor,'exist':1}
        else:
            return {'id':query[0][0],'exist':0}
    except Exception as e:
        postgresdb.rollback()
        print(f'{e}')
        return {'id':'','exist':0}


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
    except Exception as e:
        print(f'{e}')
        postgresdb.rollback()
        print('Erro al ingresar el sitio')
        return 0
def input_edited_sensor(sensor : sensorPage):
    try:
        postgres.execute("UPDATE sensor SET siteref= %s , equipref= %s, type= %s, description= %s WHERE id= %s",
                         (sensor.siteRef, sensor.equipRef, sensor.type, sensor.description, sensor.id))
        postgresdb.commit()
        return 1
    except Exception as e:
        postgresdb.rollback()
        print(f'{e}')
        return 0
def getSites(max, index):
    try:
        postgres.execute("SELECT * FROM site ORDER BY id DESC")
        q = postgres.fetchall()
        lista= []
        for i in q:
            lista.append({'id': i[0], 'site': i[1]})
        return {'data': lista[index * max: max * (index + 1)], 'indexs': list(range(math.ceil(len(lista) / max)))}
    except Exception as e:
        postgresdb.rollback()
        print(f'{e}')
        return {'data':{'id': '', 'site': ''},'indexs':[]}

def getAllSensors(max: int, index: int):
    try:
        postgres.execute("SELECT * FROM sensor ORDER BY description DESC")
        q = postgres.fetchall()
        lista= []
        for i in q:
            lista.append({'id': i[0], 'siteref': i[1], 'equipref': i[2], 'type': i[3], 'description': i[4]})
        return {'data': lista[index * max: max * (index + 1)], 'indexs': list(range(math.ceil(len(lista) / max)))}
    except Exception as e:
        postgresdb.rollback()
        print(f'{e}')
        return [{'id': '', 'siteref': '', 'equipref': '', 'type': '', 'description': ''}]


def getEquips(max, index):
    try:
        postgres.execute("SELECT * FROM equip ORDER BY id DESC")
        q = postgres.fetchall()
        lista= []
        for i in q:
            lista.append({'id': i[0], 'siteref': i[1], 'equip': i[2]})
        return {'data': lista[index * max: max * (index + 1)], 'indexs': list(range(math.ceil(len(lista) / max)))}
    except Exception as e:
        postgresdb.rollback()
        print(f'{e}')
        return {'data': {'id': '', 'siteRef': '', 'equip': ''}, 'indexs':[]}
def deleteSite(site_id: str):
    try:
        postgres.execute("SELECT * FROM site WHERE LOWER(id) = %s", (site_id.lower(),))
        query = postgres.fetchall()
        if(len(query)>0):
            postgres.execute("DELETE FROM site WHERE id = %s", (site_id,))
            postgresdb.commit()
            return {"message": "eliminado correctamente"}
        else:
            return {"message": "no encontrado"}
    except Exception as e:
        postgresdb.rollback()
        raise HTTPException(status_code=500, detail="Error al eliminar")
def deleteEquip(equip_id: str):
    try:
        postgres.execute("SELECT * FROM equip WHERE LOWER(id) = %s", (equip_id.lower(),))
        query = postgres.fetchall()
        if(len(query)>0):
            postgres.execute("DELETE FROM equip WHERE id = %s", (equip_id,))
            postgresdb.commit()
            return {"message": "eliminado correctamente"}
        else:
            return {"message": "no encontrado"}
    except Exception as e:
        postgresdb.rollback()
        raise HTTPException(status_code=500, detail="Error al eliminar")
def deleteSensor(sensor_id: str):
    try:
        postgres.execute("SELECT * FROM sensor WHERE LOWER(id) = %s", (sensor_id.lower(),))
        query = postgres.fetchall()
        if(len(query)>0):
            postgres.execute("SELECT * FROM sensor_tag WHERE LOWER(sensor_id) = %s", (sensor_id.lower(),))
            query = postgres.fetchall()
            if(len(query)>0):
                postgres.execute("DELETE FROM sensor_tag WHERE sensor_id = %s", (sensor_id,))
                postgresdb.commit()
            postgres.execute("DELETE FROM sensor WHERE id = %s", (sensor_id,))
            postgresdb.commit()
            return {"message": "eliminado correctamente"}
        else:
            return {"message": "no encontrado"}
    except Exception as e:
        postgresdb.rollback()
        raise HTTPException(status_code=500, detail="Error al eliminar")

def get_tags():
    try:
        postgres.execute("SELECT * FROM tag")
        query = postgres.fetchall()
        resp = []
        for a,b in query:
            resp.append({'id':a, 'tag':b})
        return resp
    except requests.exceptions.RequestException as e:
        postgresdb.rollback()
        print(f"Error al hacer la solicitud GET: {e}")
def get_Haystack_tags():
    try:
        return tages
    except requests.exceptions.RequestException as e:
        print(f"Error al hacer la solicitud GET: {e}")
def get_last_data(d: dataLast):
    try:
        less = (datetime.datetime.now(pytz.utc) - datetime.timedelta(hours=1))
        plux = datetime.datetime.now(pytz.utc) + datetime.timedelta(hours=1)
        final = []
        for tag in d.tags:
            query = {
                'sensedAt': {'$gte': datetime.datetime.strptime(less.strftime('%d/%m/%Y %H:%M:%S'), '%d/%m/%Y %H:%M:%S'),
                                  '$lt': datetime.datetime.strptime(plux.strftime('%d/%m/%Y %H:%M:%S'),'%d/%m/%Y %H:%M:%S')},
                     'id_sensor': d.id, 'type':tag}
            resultado = pd.DataFrame(list(c_sensor.find(query, {"_id":0, "data":1, "type":1, "sensedAt":1}).sort('sensedAt', pymongo.DESCENDING).limit(1)))
            if resultado.size> 0:
                final.append(resultado.to_dict(orient='records')[0])
            else:
                final= [{}]
        return final
    except requests.exceptions.RequestException as e:
        print(f"Error al hacer la solicitud GET: {e}")
try:
    response = requests.get('https://project-haystack.org/download/defs.json')
    if response.status_code == 200:
        datos= response.json()
        for obj in datos['rows']:
            if obj and 'prefUnit' in obj:
                json_data = {
                    "tag": obj['def']['val'],
                    'desc': obj['doc'],
                    "unit": obj['prefUnit']

                }
                tagList.append(obj['def']['val'])
                tages.append(json_data)
except requests.exceptions.RequestException as e:
    print('Error conel acceso a haystack')