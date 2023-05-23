from fastapi import FastAPI
from typing import Union
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import datetime
from dateutil.relativedelta import relativedelta
import pytz

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