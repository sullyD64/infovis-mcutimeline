# settings.py
import logging as log
from pathlib import Path

ROOT = Path.cwd() / 'data'
INPUT = ROOT / 'input'
INPUT_CRAWLED = INPUT / 'crawled'
INPUT_MANUAL = INPUT / 'manual'
INPUT_RAW = INPUT / 'raw'
OUTPUT = ROOT / 'output'

log.basicConfig(
    format=f'%(asctime)-8s | %(levelname)-7s | %(filename)-17s | %(funcName)-15s | %(message)s',
    datefmt='%H:%M:%S',
    level=log.DEBUG
)

print('%-8s | %-7s | %-17s | %-15s | %s' % ('time', 'level', 'filename', 'funcName', 'message'))
print('-' * 150)
