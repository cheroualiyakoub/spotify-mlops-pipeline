from dagster import resource
from kaggle.api.kaggle_api_extended import KaggleApi
import os

@resource
def kaggle_api():
    api = KaggleApi()
    api.authenticate()
    return api