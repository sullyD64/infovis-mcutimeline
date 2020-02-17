# data_scripts/s4_normalize.py

import logging as log

from data_scripts.lib.actions import Actions
from data_scripts.lib.constants import OUTPUT
from data_scripts.lib.extractor import Extractor
from data_scripts.lib.structs import Event, Ref, Source
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
    extr_sources = (Extractor(infile = next(OUTPUT.glob('*__final_sources.json')))
        .mapto(lambda raw_src: Source.from_dict(**raw_src))
    )
    # extr_sources_hierarchy = (Extractor(infile = next(OUTPUT.glob('*__timeline_hierarchy.json'))))

    # (extr_events
    #     .fork()
    #     .sort('date')
    #     .groupby('date')
    #     .addattr('count_events', lambda d: len(d['elements']), use_element=True)
    #     .remove_cols(['elements'])
    #     .sort('count_events', reverse=True)
    #     .save('events_bydate')
    # )

    # (extr_events
    #     .fork()
    #     .filter_rows(lambda ev: ev.date == 'May 31st, 2018')
    #     .save('events_31_05_2018')
    # )

    # extr_refs_multisrc = (extr_refs
    #     .fork()
    #     .count('all refs')
    #     .filter_rows(lambda ref: len(ref.sources) > 1)
    #     .remove_cols(['events', 'desc'])
    #     .save('refs_multisource')
    # )

    # rids_multisrc = (extr_refs_multisrc
    #     .fork()
    #     .consume_key('rid')
    #     .get()
    # )

    # (extr_events
    #     .fork()
    #     .filter_rows(lambda ev: any([rid in rids_multisrc for rid in ev.refs]))
    #     .save('events_linkedto_refs_multisource_any')
    # )

    # (extr_events
    #     .fork()
    #     .filter_rows(lambda ev: all([rid in rids_multisrc for rid in ev.refs]))
    #     .save('events_linkedto_refs_multisource_all')
    # )

    (extr_refs
        .fork()
        .filter_rows(lambda ref: not ref.is_secondary)
        .addattr('#events', lambda ref: len(ref.events))
        .select_cols(['rid', '#events'])
        # .filter_rows(lambda ref: len(ref.sources) > 1)
        .save('refs_primary')
    )
        
    (extr_refs
        .fork()
        .filter_rows(lambda ref: ref.is_secondary)
        .remove_cols(['desc'])
        .save('refs_secondary')
    )

    (extr_sources
        .fork()
        .filter_rows(lambda src: len(src.refs_primary) > 1)
        .select_cols(['sid', 'refs_primary', 'refs_secondary'])
        .save('sources_multirefsprimary')
    )






if __name__ == "__main__":
    config()
    main()
