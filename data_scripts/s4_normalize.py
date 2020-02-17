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
        .count('events')
    )
    extr_refs = (Extractor(infile = next(OUTPUT.glob('*__final_refs.json')))
        .mapto(lambda raw_ref: Ref.from_dict(**raw_ref))
        .count('refs')
    )
    extr_sources = (Extractor(infile = next(OUTPUT.glob('*__final_sources.json')))
        .mapto(lambda raw_src: Source.from_dict(**raw_src))
        .count('sources')
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

    sources_multirefsprimary_rids = (extr_sources
        .fork()
        .filter_rows(lambda src: len(src.refs_primary) > 1)
        .select_cols(['sid', 'type', 'refs_primary', 'refs_secondary'])
        .save('sources_multirefsprimary')
        .fork()
        .consume_key('refs_primary')
        .flatten()
        .sort()
        .unique()
        .get()
    )

    import re
    patterns = [
        r"^(?:<i>)?(\[\[[^\<\>]*\]\])(?:</i>)?$",
        r"^(?:<i>)?(\[\[[^\<\>]*\]\])(?:</i>)?, Volume"
    ]
    patterns_or = re.compile(f'({"|".join(patterns)})')

    sources_multirefsprimary_rids__refs_sids = (extr_refs
        .fork()
        .filter_rows(lambda ref: ref.rid in sources_multirefsprimary_rids)
        .count('before filtering')
        .filter_rows(lambda ref: re.match(patterns_or, ref.desc))
        .save('sources_multirefsprimary__refs')
        .consume_key('source')
        .sort()
        .unique()
        .get()
    )
    log.info(sources_multirefsprimary_rids__refs_sids)

    (extr_sources
        .fork()
        .filter_rows(lambda src: not src.sid in sources_multirefsprimary_rids__refs_sids)
        .save('sources_multirefsprimary__refs__sources')
    )

    (extr_events
        .fork()
        .filter_rows(lambda ev: len(ev.refs) >= 3)
        .save('events_morethan2refs')
    )


    (extr_refs
        .addattr('matches', lambda ref: bool(re.match(patterns_or, ref.desc)))
    )


    refs = extr_refs.get()

    def s4__mapto__events_add_ref_details(event: Event):
        for ref in refs:
            if event.eid in ref.events:
                detailed_ref = Ref(empty=True)
                detailed_ref.rid = ref.rid
                detailed_ref.name = ref.name
                detailed_ref.source = ref.source
                setattr(detailed_ref, 'matches', ref.matches)
                detailed_ref.desc = ref.desc[:200]

                index = event.refs.index(ref.rid)
                event.refs[index] = detailed_ref
        return event

    (extr_events
        .mapto(s4__mapto__events_add_ref_details)
        .save('events_refdetails')
        .fork()
        .filter_rows(lambda ev: not any([ref.matches for ref in ev.refs]))
        .save("events_refdetail_nonematches")
    )


if __name__ == "__main__":
    config()
    main()
