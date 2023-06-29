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
    description: str
    siteRef: str
    equipRef: str
    type: str
    data: str
class sensores(BaseModel):
    description: str
    siteRef: str
    equipRef: str
    type: str
    data: dict
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
@app.post("/sensor/")
async def create_item(item: sensor):
    input_data(item)
@app.post("/sensores/")
async def create_items(item: sensores):
    input_multiple_data(item)
    return None
@app.get('/getSensors')
async def get(id : str = '', name : str = '', max: str= '10', index: str = '0' ):
    json_compatible_item_data = jsonable_encoder(getSensors(id,name, int(max), int(index)))
    return JSONResponse(content=json_compatible_item_data)
@app.get("/getData")
async def get(id: str, start: str, end: str):
    json_compatible_item_data = jsonable_encoder(getData(id,start, end))
    return json_compatible_item_data


