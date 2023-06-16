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

