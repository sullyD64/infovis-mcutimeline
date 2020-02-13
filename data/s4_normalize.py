# s4_normalize.py
import logging as log
from settings import OUTPUT
from py_model.structs import Event
from py_logic.extractor import Extractor

CODE = 's4'

def main():
    # log.getLogger().setLevel(log.INFO)
    Extractor.code(CODE)
    Extractor.cd(OUTPUT)

    extr_events = (Extractor(infile = next(OUTPUT.glob('*__events_stripped.json')))
        .mapto(lambda raw_event: Event.from_dict(**raw_event))
    )
    # extr_refs = (Extractor(infile = next(OUTPUT.glob('*__refs_all_2.json'))))
    # extr_sources = (Extractor(infile = next(OUTPUT.glob('*__sources_refs.json'))))
    # extr_sources_hierarchy = (Extractor(infile = next(OUTPUT.glob('*__timeline_hierarchy.json'))))

    (extr_events
        .fork()
        .sort('date')
        .groupby('date')
        .addattr('count_events', lambda d: len(d['elements']), use_element=True)
        # .addattr('events:', lambda d: [ev.eid for ev in d['elements']], use_element=True)
        .remove_cols(['elements'])
        .sort('count_events', reverse=True)
        .save('events_bydate')
    )

    (extr_events
        .fork()
        .filter_rows(lambda ev: ev.date == 'May 31st, 2018')
        .save('events_31_05_2018')
    )








if __name__ == "__main__":
    main()
