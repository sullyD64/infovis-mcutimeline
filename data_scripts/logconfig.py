# data_scripts/logconfig.py

import logging

def config():
    logging.basicConfig(
        format=f'%(asctime)-8s | %(levelname)-7s | %(filename)-17s | %(funcName)-15s | %(message)s',
        datefmt='%H:%M:%S',
        level=logging.DEBUG
    )
    print('%-8s | %-7s | %-17s | %-15s | %s' % ('time', 'level', 'filename', 'funcName', 'message'))
    print('-' * 150)
