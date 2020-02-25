# data_scripts/s4_normalize.py
import logging as log
import re

from data_scripts import logconfig
from data_scripts.lib import constants, structs
from data_scripts.lib.logic import Extractor
from data_scripts.lib.pipeline import Actions

CODE = 's4'
OUTPUT = constants.PATH_OUTPUT

clean = True

def main():
    log.getLogger().setLevel(log.INFO)
    Extractor.code(CODE)
    Extractor.cd(OUTPUT)
    actions = Actions()

    if 'clean' in globals():
        Extractor.clean_output()


    extr_events = (Extractor(infile = next(OUTPUT.glob('*__final_events.json')))
        .parse_raw(structs.Event)
        .count('events')
    )

    patterns = [
        r"^(?:<i>)?(\[\[[^\<\>]*\]\])(?:</i>)?$",        # match full string containing just the link
        # r"^In <i>(\[\[[^\<\>]*\]\])</i>",                # match string beginning with "In {link}"
        r"^(?:<i>)?(\[\[[^\<\>]*\]\])(?:</i>)?, Volume"  # match string containing the link, followed by a Volume reference
]

    extr_refs = (Extractor(infile = next(OUTPUT.glob('*__final_refs.json')))
        .parse_raw(structs.Ref)
        .addattr('num_events', lambda ref: len(ref.events))
        .addattr('matches', lambda ref: bool(re.match(re.compile(f'({"|".join(patterns)})'), ref.desc)))
        .count('refs')
    )

    extr_sources = (Extractor(infile = next(OUTPUT.glob('*__final_sources.json')))
        .parse_raw(structs.Source)
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
        .save('refs_simple')
    )
    (extr_refs_shortdesc
        .fork()
        .remove_cols(['events'])
        .filter_rows(lambda ref: ref.complex)
        .save('refs_complex')
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
        .filter_rows(lambda ev: len(ev.reflinks) >= 2)
        .select_cols(['eid', 'desc', 'reflinks'])
        .save('events_morethan2reflinks')
    )

    # NOT NEEDED (we removed duplicate Sources with same sid)
    # (extr_sources
    #     .fork()
    #     .groupby('sid')
    #     .filter_rows(lambda group: len(group['elements']) > 1)
    #     .save('sources_bysid')
    # )

    (extr_events
        .fork()
        .filter_rows(lambda ev: not ev.reality == constants.REALITY_MAIN)
        .save('events_altrealities')
    )
    (extr_events
        .filter_rows(lambda ev: ev.reality == constants.REALITY_MAIN)
        .save('events_mainreality')
    )

    (extr_events
        .fork()
        .consume_key('date')
        .unique()
        .sort()
        .save('legend__events_dates')
    )
    (extr_events
        .fork()
        .select_cols(['eid', 'date', 'reality', 'sources', 'desc', 'characters'])
        .filter_rows(lambda ev: any([ev.date.endswith(year) for year in ['2017', '2018']]))
        .sort('date')
        .groupby('date')
        .addattr('num_events', lambda group: len(group['elements']))
        .addattr('elements', lambda group: Extractor(data=group['elements']).sort('sources').groupby('sources').get())
        .filter_rows(lambda group: group['num_events'] > 10)
        .save('events_bydate')
    )
 
    # -----------------------------------------------

    # TODO move to S3

    extr_allchars = (Extractor(infile=next(OUTPUT.glob('*__allchars.json'))))
    oc = Extractor(infile=next(OUTPUT.glob('*__occ_chars.json'))).get_first()
    
    # normalize characters, adding missing keys with empty values and including the number of occurrences
    (extr_allchars
        .mapto(actions.s3__mapto__chars__normalize_missing_attributes)
        .addattr('num_occurrences', lambda char: oc[char['cid']])
        .save('allchars_normalized')
    )

    # add link to parent source
    # add characters to sources based on the 'appearence' key
    # TODO copiare i characters di tutte le sub-source nella source root
    (extr_sources
        .addattr('parent', actions.s3__addattr__sources__parent_source)
        .addattr('characters', actions.s3__addattr__sources__characters, **{'allchars': extr_allchars.get()})
        .save('sources_characters')
    )
    
    # discover new characters in event descriptions using character ids and names
    actions.set_counters(*['cnt_updated'])
    (extr_events
        .fork()
        .addattr('characters', actions.s3__addattr__events__discover_characters, **{
            'sources': extr_sources.get(),
            'sources_index': extr_sources.get_index('sid'),
        })
        .save('events_witchars_additional')
    )
    cntrs = actions.get_counters()
    log.info(f'-- Updated events: {cntrs["cnt_updated"]}')

    (extr_events
        .iterate(actions.s3__iterate__events__merge_consecutive_similar_events)
    )

if __name__ == "__main__":
    logconfig.config()
    main()
