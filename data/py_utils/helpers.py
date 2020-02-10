# py_utils/helpers.py
import json

def jloads(rawfile):
    return json.loads(rawfile.read())

def jdumps(obj):
    return json.dumps(obj, indent=2, ensure_ascii=False)

