# data_scripts/s4_normalize.py

import logging as log

from data_scripts.lib.actions import Actions
from data_scripts.lib.constants import OUTPUT
from data_scripts.lib.extractor import Extractor
from data_scripts.lib.structs import Event, Ref
from data_scripts.logconfig import config

CODE = 's4'
clean = True

def main():
    # log.getLogger().setLevel(log.INFO)
    Extractor.code(CODE)
    Extractor.cd(OUTPUT)
    actions = Actions()

    if 'clean' in globals():
        Extractor.clean_output()

    extr_events = (Extractor(infile = next(OUTPUT.glob('*__final_events.json')))
        .mapto(lambda raw_event: Event.from_dict(**raw_event))
    )
    extr_refs = (Extractor(infile = next(OUTPUT.glob('*__final_refs.json')))
        .mapto(lambda raw_ref: Ref.from_dict(**raw_ref))
    )
    # extr_sources = (Extractor(infile = next(OUTPUT.glob('*__final_sources.json'))))
    # extr_sources_hierarchy = (Extractor(infile = next(OUTPUT.glob('*__timeline_hierarchy.json'))))

    (extr_events
        .fork()
        .sort('date')
        .groupby('date')
        .addattr('count_events', lambda d: len(d['elements']))
        # .addattr('events:', lambda d: [ev.eid for ev in d['elements']])
        .remove_cols(['elements'])
        .sort('count_events', reverse=True)
        .save('events_bydate')
    )

    (extr_events
        .fork()
        .filter_rows(lambda ev: ev.date == 'May 31st, 2018')
        .save('events_31_05_2018')
    )




    # TODO 3) convert refs in a m2m table (with links to ref details)








if __name__ == "__main__":
    config()
    main()
