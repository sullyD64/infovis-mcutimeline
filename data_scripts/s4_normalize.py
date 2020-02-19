# data_scripts/s4_normalize.py

import logging as log
import re

from data_scripts.lib.actions import Actions
from data_scripts.lib.constants import OUTPUT
from data_scripts.lib.extractor import Extractor
from data_scripts.lib.structs import Event, Ref, Source
from data_scripts.logconfig import config

CODE = 's4'
clean = True

def main():
    log.getLogger().setLevel(log.INFO)
    Extractor.code(CODE)
    Extractor.cd(OUTPUT)
    actions = Actions()

    if 'clean' in globals():
        Extractor.clean_output()


    extr_events = (Extractor(infile = next(OUTPUT.glob('*__final_events.json')))
        .mapto(lambda raw_event: Event.from_dict(**raw_event))
        .count('events')
    )

    patterns = [
        r"^(?:<i>)?(\[\[[^\<\>]*\]\])(?:</i>)?$",        # match full string containing just the link
        # r"^In <i>(\[\[[^\<\>]*\]\])</i>",                # match string beginning with "In {link}"
        r"^(?:<i>)?(\[\[[^\<\>]*\]\])(?:</i>)?, Volume"  # match string containing the link, followed by a Volume reference
]

    extr_refs = (Extractor(infile = next(OUTPUT.glob('*__final_refs.json')))
        .mapto(lambda raw_ref: Ref.from_dict(**raw_ref))
        .addattr('num_events', lambda ref: len(ref.events))
        .addattr('matches', lambda ref: bool(re.match(re.compile(f'({"|".join(patterns)})'), ref.desc)))
        .count('refs')
    )

    extr_sources = (Extractor(infile = next(OUTPUT.glob('*__final_sources.json')))
        .mapto(lambda raw_src: Source.from_dict(**raw_src))
        .count('sources')
    )

    # extr_sources_hierarchy = (Extractor(infile = next(OUTPUT.glob('*__timeline_hierarchy.json'))))

    # ================================

    (extr_events
        .fork()
        .sort('date')
        .groupby('date')
        .addattr('num_events', lambda d: len(d['elements']))
        .remove_cols(['elements'])
        .sort('num_events', reverse=True)
        .save('events_bydate')
    )

    (extr_events
        .fork()
        .filter_rows(lambda ev: ev.date == 'May 31st, 2018')
        .save('events_31_05_2018')
    )

    # NOT NEEDED (we removed multi-source refs)
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

    extr_refs_shortdesc = (extr_refs
        .fork()
        .addattr('desc', lambda ref: ref.desc[:200])
    )

    (extr_refs_shortdesc
        .fork()
        .remove_cols(['events'])
        # .filter_rows(lambda ref: ref.num_events in range(1,5))
        .sort('num_events', reverse=True)
        .save('refs_ranked')
    )

    (extr_refs_shortdesc
        .fork()
        .remove_cols(['events'])
        .filter_rows(lambda ref: not ref.complex)
        .save('refs_primary')
    ) 
    (extr_refs_shortdesc
        .fork()
        .remove_cols(['events'])
        .filter_rows(lambda ref: ref.complex)
        .save('refs_secondary')
    )

    # NOT NEEDED (there are no sources with multiple primary refs)
    # sources_multirefsprimary_rids = (extr_sources
    #     .fork()
    #     .filter_rows(lambda src: len(src.refs_primary) > 1)
    #     .select_cols(['sid', 'type', 'refs_primary'])
    #     .save('sources_multirefsprimary')
    #     .consume_key('refs_primary')
    #     .flatten()
    #     .unique()
    #     .sort()
    #     .save('sources_multirefsprimary_rids')
    #     .get()
    # )
    # multirefsprimary_sids = (extr_refs_shortdesc
    #     .fork()
    #     .filter_rows(lambda ref: ref.rid in sources_multirefsprimary_rids)
    #     .save('sources_multirefsprimary_rids_refs')
    #     .consume_key('source')
    #     .unique()
    #     .sort()
    #     .save('sources_multirefsprimary_rids_refs_sids')
    #     .get()
    # )
    # (extr_sources
    #     .fork()
    #     .filter_rows(lambda src: src.sid in multirefsprimary_sids)
    #     .save('sources_multirefsprimary_rids_refs_sources')
    # )

    # ---------------

    (extr_refs_shortdesc.count('all refs'))

    (extr_refs_shortdesc
        .fork()
        .filter_rows(lambda ref: ref.matches and ref.complex)
        .remove_cols(['events', 'matches', 'complex'])
        .save('refs_bool_00')
    )
    (extr_refs_shortdesc
        .fork()
        .filter_rows(lambda ref: ref.matches and not ref.complex)
        .remove_cols(['events', 'matches', 'complex'])
        .save('refs_bool_01')
    )
    (extr_refs_shortdesc
        .fork()
        .filter_rows(lambda ref: not ref.matches and ref.complex)
        .remove_cols(['events', 'matches', 'complex'])
        .save('refs_bool_10')
    )
    (extr_refs_shortdesc
        .fork()
        .filter_rows(lambda ref: not ref.matches and not ref.complex)
        .remove_cols(['events', 'matches', 'complex'])
        # .consume_key('desc')
        # .filter_rows(lambda ref: not re.match(r"In <i>(\[\[[^\<\>]*\]\])</i>", ref.desc))
        .save('refs_bool_11')
    )

    """
    +-----+-----------------------------+-----------------------------+-----------+
    |     |              0              |              1              |    tot    |
    +-----+--------------+--------------+--------------+--------------+-----+-----+
    |  0  | matches      | matches      | matches      | matches      | 912 | 513 |
    |     | complex      | complex      | not complex  | not complex  |     |     |
    |     | p123 [228]   | p13 [8]      | p123 [684]!  | p13 [505]!   |     |     |
    |     +--------------+--------------+--------------+--------------+-----+-----+
    |     | matches      |              | matches      |              | 404 |     |
    |     | complex      |              | not complex  |              |     |     |
    |     | p2 [220]     |              | p2 [184]     |              |     |     |
    +-----+--------------+--------------+--------------+--------------+-----+-----+
    |  1  | not matches  | not matches  | not matches  | not matches  | 142 | 541 |
    |     | complex      | complex      | not complex  | not complex  |     |     |
    |     | p123 [125]   | p13 [345]    | p123 [17]    | p13 [196]    |     |     |
    |     +--------------+--------------+--------------+--------------+-----+-----+
    |     | not matches  |              | not matches  |              | 650 |     |
    |     | complex      |              | not complex  |              |     |     |
    |     | p2 [133]     |              | p2 [517]!    |              |     |     |
    +-----+--------------+--------------+--------------+--------------+-----+-----+
    | tot |             353             |             701             |    1054   |
    +-----+-----------------------------+-----------------------------+-----------+
    """

    # NOT NEEDED (we removed Refs from Events)
    # def s4__mapto__events_add_ref_details(event: Event):
    #     for ref in extr_refs_shortdesc.get():
    #         if event.eid in ref.events:
    #             detailed_ref = Ref(empty=True)
    #             detailed_ref.rid = ref.rid
    #             detailed_ref.name = ref.name
    #             detailed_ref.source = ref.source
    #             detailed_ref.complex = ref.complex
    #             setattr(detailed_ref, 'matches', ref.matches)
    #             detailed_ref.desc = ref.desc
    #             index = event.refs.index(ref.rid)
    #             event.refs[index] = detailed_ref
    #     return event

    # (extr_events
    #     .mapto(s4__mapto__events_add_ref_details)
    #     .save('events_refdetails')
    #     .fork()
    #     .filter_rows(lambda ev: not ev.characters and not ev.non_characters)
    #     .save("events_nolinks")
    # )
    
    (extr_events
        .fork()
        .filter_rows(lambda ev: len(ev.sources) >= 3)
        .select_cols(['eid', 'desc', 'sources'])
        .save('events_morethan2sources')
    )


if __name__ == "__main__":
    config()
    main()
