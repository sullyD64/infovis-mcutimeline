# py_utils/upload.py

import pymongo
import os

from py_logic.extractor import Extractor

# TODO work in progress

mongo = pymongo.MongoClient("mongodb://localhost:27017/")
db = mongo['mcu']
DIR = os.path.dirname(__file__)

def drop_all():
    mongo.drop_database(db)

drop_all()
db = mongo['mcu']

inpath = os.path.join(DIR, f'auto/extracted__29__events_stripped.json')
events_data = (Extractor(inpath)
    .addattr('_id', lambda **kwargs: kwargs['element']['eid'], use_element=True)
    .remove_cols(['eid'])
    .get()
)

events_col = db['events']
for event in events_data[:10]:
    events_col.insert_one(event)


inpath = os.path.join(DIR, f'auto/extracted__22__refs_all_2.json')
refs_data = (Extractor(inpath)
    .addattr('_id', lambda **kwargs: kwargs['element']['rid'], use_element=True)
    .remove_cols(['rid'])
    .get()
)

refs_col = db['refs']
for ref in refs_data[:10]:
    refs_col.insert_one(ref)

