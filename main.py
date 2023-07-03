import json

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
    data: list
    sensedAt : str
app = FastAPI(
    title= 'HayIoT', description= 'Integradora', version= '1.1.0'
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
@app.post("/pushSensor/")
async def create_items(item: sensor):
    return input_page_data_sensor(item)
@app.post("/pushEquip/")
async def create_items(item: equipo):
    return input_page_equip(item)
@app.get('/getSensors')
async def get(id : str = '', name : str = '', max: str= '10', index: str = '0' ):
    json_compatible_item_data = jsonable_encoder(getSensors(id,name, int(max), int(index)))
    return JSONResponse(content=json_compatible_item_data)
@app.get("/getData")
async def get(id: str, start: str, end: str):
    json_compatible_item_data = jsonable_encoder(getData(id,start, end))
    return json_compatible_item_data


