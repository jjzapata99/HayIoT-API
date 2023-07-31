import asyncio
import concurrent.futures
import json
from typing import Union

from fastapi import FastAPI
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
app = FastAPI(
    title= 'HayIoT', description= 'Integradora', version= '1.1.0',
    redoc_url=None

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

@app.post("/sensores/")
async def create_items(item: sensores):
    return input_multiple_data(item)
@app.get("/getData")
async def get(id: str, start: str, end: str):
     return getData(id,start, end)

@app.get("/getHaystackTags")
async def get():
    json_compatible_item_data = jsonable_encoder(get_Haystack_tags())
    return json_compatible_item_data
@app.get('/getSensors', include_in_schema=False)
async def get(id : str = '', name : str = '', max: str= '10', index: str = '0' ):
    json_compatible_item_data = jsonable_encoder(getSensors(id,name, int(max), int(index)))
    return JSONResponse(content=json_compatible_item_data)
@app.get("/getLastSensed", include_in_schema=False)
async def create_items(id:str):
    return getLastDate(id)
@app.post("/pushSensor/", include_in_schema=False)
async def create_items(item: sensor):
    return input_page_data_sensor(item)
@app.post("/pushEquip/", include_in_schema=False)
async def create_items(item: equipo):
    return input_page_equip(item)
@app.post("/pushSite/",include_in_schema=False)
async def create_items(item: site):
    return input_page_site(item)
@app.post('/editDataSensor/', include_in_schema=False)
async def get(item: sensorPage):
    return (input_edited_sensor(item))
@app.get('/getSites',include_in_schema=False)
async def get(max: Union[str, None]= '10', index: Union[str, None] = '0'):
    json_compatible_item_data = jsonable_encoder(getSites(int(max), int(index)))
    return JSONResponse(content=json_compatible_item_data)
@app.get('/getEquips',include_in_schema=False)
async def get(max: Union[str, None]= '10', index: Union[str, None] = '0'):
    json_compatible_item_data = jsonable_encoder(getEquips(int(max), int(index)))
    return JSONResponse(content=json_compatible_item_data)
@app.get('/getAllSensors',include_in_schema=False)
async def get(max: Union[str, None]= '10', index: Union[str, None] = '0'):
    json_compatible_item_data = jsonable_encoder(getAllSensors(int(max), int(index)))
    return JSONResponse(content=json_compatible_item_data)

@app.delete('/deleteSite', include_in_schema=False)
async def delete(id: str):
    try:
        result = deleteSite(id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al eliminar el sensor")
@app.delete('/deleteEquip', include_in_schema=False)
async def delete(id: str):
    try:
        result = deleteEquip(id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al eliminar el sensor")
@app.delete('/deleteSensor', include_in_schema=False)
async def delete(id: str):
    try:
        result = deleteSensor(id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al eliminar el sensor")