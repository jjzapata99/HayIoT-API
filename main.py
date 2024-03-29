import asyncio
import concurrent.futures
import json
from typing import Union
import ssl
from fastapi import FastAPI, Response
from typing import Union
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import datetime
from dateutil.relativedelta import relativedelta
import pytz

from backend import *

class dataModel(BaseModel):
    val: float
    type: str
class dataLast(BaseModel):
    id: str
    tags: list[str]
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
class equipo(BaseModel):
    id: str
    siteRef: str
    equip: str
class sensores(BaseModel):
    id: str
    data: list[dataModel]
    sensedAt : Union[str, None]
class site(BaseModel):
    id: str
    site: str
class dataWeb(BaseModel):
    id: str
    start: str
    end: str
    tags: Union[list[str], None]
app = FastAPI(
    title= 'HayIoT', description= 'Proyecto HayIoT, estandarización de sensores', version= '0.0.1', openapi_url= "/api/hayiot/openapi.json",
    redoc_url=None, docs_url="/api/hayiot/docs"

)
origins = [
    "*"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
baseUrl = "/api/hayiot"
@app.post(baseUrl+"/sensores/")
async def create_items(item: sensores):
    return input_multiple_data(item)
@app.get(baseUrl+"/getData")
def get(id: str, start: str, end: str):
    return Response(getData(id,start, end), media_type='application/json')
@app.get(baseUrl+"/downloadData")
def get(id: str, start: str, end: str):
     headers = {'Content-Disposition': 'attachment; filename="data.json"'}
     return Response(getData(id,start, end), headers=headers, media_type='application/json')
@app.get(baseUrl+"/getSpecificData")
def get(id: str, start: str, end: str, type:str):
    return Response(getSpecificData(id,start, end, type), media_type="application/json")
@app.post(baseUrl+"/getDataWeb", include_in_schema=False)
def post(data : dataWeb):
    return Response(getDataWeb(data), media_type="application/json")

@app.get(baseUrl+"/getHaystackTags")
async def get():
    json_compatible_item_data = jsonable_encoder(get_Haystack_tags())
    return json_compatible_item_data
@app.get(baseUrl+"/getTags", include_in_schema=False)
async def get():
    json_compatible_item_data = jsonable_encoder(get_tags())
    return json_compatible_item_data
@app.post(baseUrl+"/getLastData", include_in_schema=False)
async def get(d:dataLast):
    json_compatible_item_data = jsonable_encoder(get_last_data(d))
    return json_compatible_item_data
@app.get(baseUrl+'/getSensors', include_in_schema=False)
async def get(id : str = '', name : str = '', max: str= '10', index: str = '0' ):
    json_compatible_item_data = jsonable_encoder(getSensors(id,name, int(max), int(index)))
    return JSONResponse(content=json_compatible_item_data)
@app.get(baseUrl+"/getLastSensed", include_in_schema=False)
async def create_items(id:str):
    return getLastDate(id)
@app.post(baseUrl+"/pushSensor/", include_in_schema=False)
async def create_items(item: sensor):
    return input_page_data_sensor(item)
@app.post(baseUrl+"/pushEquip/", include_in_schema=False)
async def create_items(item: equipo):
    return input_page_equip(item)
@app.post(baseUrl+"/pushSite/",include_in_schema=False)
async def create_items(item: site):
    return input_page_site(item)
@app.post(baseUrl+'/editDataSensor/', include_in_schema=False)
async def get(item: sensorPage):
    return (input_edited_sensor(item))
@app.get(baseUrl+'/getSites',include_in_schema=False)
async def get(max: Union[str, None]= '10', index: Union[str, None] = '0'):
    json_compatible_item_data = jsonable_encoder(getSites(int(max), int(index)))
    return JSONResponse(content=json_compatible_item_data)
@app.get(baseUrl+'/getEquips',include_in_schema=False)
async def get(max: Union[str, None]= '10', index: Union[str, None] = '0'):
    json_compatible_item_data = jsonable_encoder(getEquips(int(max), int(index)))
    return JSONResponse(content=json_compatible_item_data)
@app.get(baseUrl+'/getAllSensors',include_in_schema=False)
async def get(max: Union[str, None]= '10', index: Union[str, None] = '0'):
    json_compatible_item_data = jsonable_encoder(getAllSensors(int(max), int(index)))
    return JSONResponse(content=json_compatible_item_data)

@app.delete(baseUrl+'/deleteSite', include_in_schema=False)
async def delete(id: str):
    try:
        result = deleteSite(id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al eliminar el sensor")
@app.delete(baseUrl+'/deleteEquip', include_in_schema=False)
async def delete(id: str):
    try:
        result = deleteEquip(id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al eliminar el sensor")
@app.delete(baseUrl+'/deleteSensor', include_in_schema=False)
async def delete(id: str):
    try:
        result = deleteSensor(id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al eliminar el sensor")