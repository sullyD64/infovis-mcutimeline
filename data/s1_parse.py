# s1_parse.py
import logging as log

from py_logic.actions import Actions
from py_logic.extractor import Extractor
from py_logic.parser import Parser
from py_utils.constants import TIMELINE_PAGES
from py_utils.errors import RequiredInputMissingError
from settings import INPUT_RAW, OUTPUT

CODE = 's1'
clean = True

def main():
    log.getLogger().setLevel(log.INFO)
    log.info('### Begin ###')
    Extractor.code(CODE)

    filelist = TIMELINE_PAGES
    files_found = [p.stem for p in INPUT_RAW.glob('*')]
    for f in files_found:
        if f not in filelist:
            raise RequiredInputMissingError(CODE)
    # filelist = ['before-20th']
    # filelist = ['time-heist']
    # filelist = ['2018']

    Extractor.cd(OUTPUT)
    if 'clean' in globals():
        Extractor.clean_output()

    # 1. parse raw pages into an event list
    extr_events = Extractor()
    for i, fname in enumerate(filelist, start=1):
        infile = INPUT_RAW / fname
        log.info(f'Parsing [{i}/{len(filelist)}] \t{infile}')
        with open(infile) as rawfile:
            tfile = rawfile.read().splitlines()
            parsed_events = Parser().parse_timeline(tfile, fname)
            extr_events.extend(Extractor(data=parsed_events))
    
    # 2. group nested events
    (extr_events
        .count('events before grouping')
        .iterate(Actions().s1__iterate__events__group_nested)
        .count('events after grouping')
        .save('events', nostep=True)
    )
    log.info('### End ###')

if __name__ == "__main__":
    main()
