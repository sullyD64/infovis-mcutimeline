# data_scripts/s3_extract.py
import logging as log

from data_scripts import logconfig
from data_scripts.lib import constants, errors, structs
from data_scripts.lib.logic import Extractor
from data_scripts.lib.pipeline import Actions

CODE = 's3'
INPUT_MANUAL = constants.PATH_INPUT_MANUAL
OUTPUT = constants.PATH_OUTPUT

clean = True
NAMEDREFS_INCLUDE_VOID = True

def main():
    log.getLogger().setLevel(log.INFO)
    log.info('### Begin ###')
    actions = Actions()
    Extractor.code(CODE)
    Extractor.cd(OUTPUT)

    infiles_manual = next(INPUT_MANUAL.glob('*'), None)
    infiles_s1s2 = next(OUTPUT.glob('*__s*'), None)
    if not (infiles_manual or infiles_s1s2):
        raise errors.RequiredInputMissingError(CODE)

    if 'clean' in globals():
        Extractor.clean_output()

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 1. SOURCES ###')
    # --------------------------------------------------------------------

    # 1.0 load raw sources into Sources
    # 1.1 extract film and tv root sources
    # 1.2 extend sources with newly extracted root sources
    actions.set_legends(**{'root_sources': []})
    extr_sources = (Extractor(infile=next(INPUT_MANUAL.glob('films.json')))
        .extend(Extractor(infile=next(INPUT_MANUAL.glob('tv_episodes.json'))))
        .iterate(actions.s3__iterate__sources__build)
        .save('sources_0')
        .mapto(actions.s3__mapto__sources__extract_film_root_sources)
        .mapto(actions.s3__mapto__sources__extract_tv_root_sources)
        .extend(
            Extractor(data=actions.get_legend('root_sources'))
            .save('sources_0_roots')
        )
        .save('sources_1')
    )

    # 1.3 extend sources by creating new Source based on mentioned sources in crawled characters.
    # TODO NOTE that in s2 we didn't filter "unreleased" sources while parsing characters.
    # >>> if the char_source includes a clarification, the matching source's title is updated with the new found clarification.
    # >>> if no source is found, a new source is discovered and added.
    actions.set_legends(**{
        'sources': extr_sources.get(),
        'sources_updated': [],
        'sources_additional': [],
    })
    actions.set_counters(*['cnt_discovered', 'cnt_updated'])
    (Extractor(infile=next(OUTPUT.glob('*__sources_chars.json')))
        .mapto(actions.s3__mapto__sources__extract_additional_sources)
    )
    cntrs = actions.get_counters()
    log.info(f'-- Updated sources: {cntrs["cnt_updated"]}')
    log.info(f'-- Discovered sources: {cntrs["cnt_discovered"]}')

    # 1.4 merge sources after update and discover
    (extr_sources
        .count('sources before update')
        .extend(
            Extractor(data=actions.get_legend('sources_updated'))
            .save('sources_1_updated')
        )
        .extend(
            Extractor(data=actions.get_legend('sources_additional'))
            .save('sources_1_additional')
        )
        .save('sources_2')
    )

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 2. REFS ###')
    # --------------------------------------------------------------------

    # 2.0 load raw events into Events including Refs
    extr_events = (Extractor(infile=next(OUTPUT.glob('*__events_characters.json'), None))
        .parse_raw(structs.Event)
    )
    # 2.1 fork flattened list of refs
    extr_refs = (extr_events.fork()
        .consume_key('refs')
        .flatten()
        .remove_cols(['links'])
        .save('refs_all_0')
    )

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 3. ANONYMOUS REFS ###')
    # --------------------------------------------------------------------

    # 3.0 fork anonymous refs
    extr_refs_anon = (extr_refs.fork()
        .filter_rows(lambda ref: ref.name is None)
        .unique()
        .sort()
        .save('refs_anon')
    )

    patterns = [
        r"^(?:<i>)?(\[\[[^\<\>]*\]\])(?:</i>)?$",       # match full string containing just the link
        r"^In <i>(\[\[[^\<\>]*\]\])</i>",               # match string beginning with "In {link}"
        r"^(?:<i>)?(\[\[[^\<\>]*\]\])(?:</i>)?, Volume" # match string containing the link, followed by a Volume reference
    ]
    # 3.1 extract valid anonymous refs
    # 3.2 infer sourcetitle from ref desc
    # 3.3 filters invalid refs, for which it hasn't been found a proper sourcetitle and thus would be impossible to add a sourceid later on.
    actions.set_counters(*['cnt_existing', 'cnt_updated'])
    (extr_refs_anon
        .addattr('source__title', actions.s3__addattr__refs__sourcetitle, **{'patterns': patterns})
        .filter_rows(lambda ref: ref.source__title)
        .save('refs_anon_plus_sourcetitle')
    )
    cntrs = actions.get_counters()
    log.info(f'-- Added {cntrs["cnt_existing"]}/{len(extr_refs_anon.get())} sourcetitles to anonrefs. The rest has been filtered.')

    # 3.4 infer sourceid for ref by looking in sources using inferred sourcetitle
    # >>> if sourcetitle includes a clarification, the matching source's title is updated with the new found clarification.
    # >>> if no source is found using sourcetitle, a new source is discovered and added.
    actions.set_legends(**{
        'sources': extr_sources.get(),
        'sources_updated': [],
        'sources_additional': []
    })
    actions.set_counters(*['cnt_existing', 'cnt_discovered', 'cnt_updated'])
    (extr_refs_anon
        .addattr('source__id', actions.s3__addattr__anonrefs__sourceid)
        .save('refs_anon_plus_sourcetitle_sourceid')
    )
    cntrs = actions.get_counters()
    log.info(f'-- Linked {cntrs["cnt_existing"]}/{len(extr_refs_anon.get())} anonrefs to existing sources.')
    log.info(f'-- Updated sources: {cntrs["cnt_updated"]}')
    log.info(f'-- Discovered sources: {cntrs["cnt_discovered"]}')

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 4. SOURCES (update) ###')
    # --------------------------------------------------------------------

    # 4 merge sources after update and discover
    (extr_sources
        .count('sources before update')
        .extend(
            Extractor(data=actions.get_legend('sources_updated'))
            .save('sources_2_updated')
        )
        .extend(
            Extractor(data=actions.get_legend('sources_additional'))
            .save('sources_2_additional')
        )
        .save('sources_3')
    )

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 5. NAMED REFS ###')
    # --------------------------------------------------------------------

    # 5.0 fork named refs
    extr_refs_named = (extr_refs.fork()
        .filter_rows(lambda ref: ref.name is not None)
        .sort()
        .count('refs_named')
        .save('refs_named')
    )

    # 5.1 extract unique named refs
    """
    Working on unique when coding the job is the same: unique means "with same name and desc".
    De facto, each ref is unique on its own, since it refers to a different event,
    but refs in a void html tag are just pointers to non-void ref tags, which are far more relevant.
    """
    extr_refs_named_unique = (extr_refs_named.fork()
        .filter_rows(lambda ref: ref.desc is not None)
        .unique()
        .sort()
        .save('refs_named_unique')
    )

    # ------------------------------------------------------
    # MODE SWITCHER between unique-only and all named refs
    if 'NAMEDREFS_INCLUDE_VOID' not in globals():
        log.warning("** WARNING: Using unique, non-void named refs **")
        extr_refs_named = extr_refs_named_unique
    # ------------------------------------------------------

    # 5.2 infer sourcetitle from ref desc
    actions.set_counters(*['cnt_existing', 'cnt_updated'])
    (extr_refs_named
        .addattr('source__title', actions.s3__addattr__refs__sourcetitle, **{'patterns': patterns})
        .save('refs_named_plus_sourcetitle')
    )
    cntrs = actions.get_counters()
    log.info(f'-- Added {cntrs["cnt_existing"]}/{len(extr_refs_named.get())} sourcetitles to namedrefs.')
    log.info(f'-- Updated refnames: {cntrs["cnt_updated"]}')

    # 5.3 link ref to sources based on inferred sourcetitle, refname and the source's title and id
    # >>> if sourcetitle matches with existing source but it has no id, the source is updated with the new id based on the refname.
    # >>> if no source is found either using sourcetitle or refname, a new source is discovered and added.
    # IMPORTANT: two passes are necessary, because of the intrinsic asymmetricality

    # 5.3.1 first pass (perform positive matches and updates)
    log.info(f'** first pass **')
    actions.set_legends(**{
        'sources': extr_sources.get(),
        'sources_updated': [],
        'sources_additional': [],
    })
    actions.set_counters(*['cnt_existing', 'cnt_discovered', 'cnt_updated', 'cnt_ref_marked'])
    (extr_refs_named
        .addattr('source__id', actions.s3__addattr__namedrefs__sourceid_1)
        .save('refs_named_plus_sourcetitle_sourceid_1')
    )
    cntrs = actions.get_counters()
    log.info(f'-- Linked {cntrs["cnt_existing"]}/{len(extr_refs_named.get())} named refs to existing sources.')
    log.info(f'-- Updated sources: {cntrs["cnt_updated"]}')


    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 6. SOURCES (update 2) ###')
    # --------------------------------------------------------------------

    # 6 merge sources after update and discover
    (extr_sources
        .count('sources before update')
        .extend(
            Extractor(data=actions.get_legend('sources_updated'))
            .save('sources_4_updated')
        )
        .extend(
            Extractor(data=actions.get_legend('sources_additional'))
            .save('sources_4_additional')
        )
        .save('sources_5')
    )

    # 5.3.2 second pass (perform positive matches, discovery and marking)
    log.info(f'** second pass **')
    actions.set_legends(**{
        'sources': extr_sources.get(),
        'sources_updated': [],
        'sources_additional': [],
    })
    (extr_refs_named
        .addattr('source__id', actions.s3__addattr__namedrefs__sourceid_2)
        .save('refs_named_plus_sourcetitle_sourceid_2')
    )
    cntrs = actions.get_counters()
    log.info(f'-- Linked {cntrs["cnt_existing"]}/{len(extr_refs_named.get())} named refs to existing sources.')
    log.info(f'-- Discovered sources: {cntrs["cnt_discovered"]}')
    log.info(f'-- Refs marked: {cntrs["cnt_ref_marked"]}')

    # 5 extra: create a unique refnames list
    refnames = (extr_refs_named.fork()
        .consume_key('name')
        .unique()
        .sort()
        .save('refnames_legend')
        .get()
    )

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 7. SOURCES (update 3) ###')
    # --------------------------------------------------------------------

    # 7.0 merge sources after update and discover
    (extr_sources
        .count('sources before update')
        .extend(
            Extractor(data=actions.get_legend('sources_updated'))
            .save('sources_5_updated')
        )
        .extend(
            Extractor(data=actions.get_legend('sources_additional'))
            .save('sources_5_additional')
        )
        .save('sources_6')
    )

    # 7.1 filter sources for which there hasn't been found a sourceid (at this point, we don't need that sources)
    (extr_sources.fork()
        .filter_rows(lambda src: not src.sid)
        .save('sources_6_invalid')
    )
    (extr_sources
        .filter_rows(lambda src: src.sid)
        .save('sources_7')
    )

    # 7.2 group comic sources where there is a "main" source and a "volume" source
    actions.set_legends(**{'updated_sources_mapping': {}})
    (extr_sources
        .iterate(actions.s3__iterate__sources__group_comic_duplicates)
        .save('sources_7_grouped')
    )

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 8. NAMED REFS (again) ###')
    # --------------------------------------------------------------------

    # 8.0 update refs previously referring to grouped sources to new main source
    (extr_refs_named
        .mapto(actions.s3__mapto__namedrefs__update_srcid_grouped)
    )

    # 8.1 begin adding sources as lists, starting from "multiple" refnames
    actions.set_legends(**{'sources': extr_sources.get()})
    actions.set_counters(*['cnt__complex_ref_found', 'cnt__not_a_source'])
    (extr_refs_named
        .addattr('sources', [])
        .mapto(actions.s3__mapto__namedrefs__add_srcid_multiple)
        .addattr('source__title', actions.s3__addattr__namedrefs__remaining_srctitle)
        .save('refs_named_sources_1')
    )
    cnt_tot = len(extr_refs_named.get())
    cntrs = actions.get_counters()
    log.info(f'-- Out of {cnt_tot} named refs:')
    log.info(f'\t-- {cnt_tot - cntrs["cnt__complex_ref_found"] - cntrs["cnt__not_a_source"]} refs were left unchanged.')
    log.info(f'\t-- {cntrs["cnt__complex_ref_found"]} refs have a complex name (multiple tokens) but refer to a valid source, so they are complex refs.')
    log.info(f'\t\tThose are marked with {constants.TMP_MARKERS[2]} and will have complex=True')
    log.info(f'\t-- {cntrs["cnt__not_a_source"]} refs have a complex name (multiple tokens) which is invalid (not a source)')
    log.info(f'\t\tThose are marked with {constants.TMP_MARKERS[1]}. Those refs (and the corresponding events) will be later deleted.')

    # 8.2 finish converting source ids into lists and cleanup
    # >>> add complex=True to the refs with multiple tokens but referring to a valid source.
    (extr_refs_named
        .addattr('complex', actions.s3__addattr__namedrefs__complex)
        .mapto(actions.s3__mapto__refs__convert_srcid_srctitle_to_sourcelist)
        .remove_cols(['source__title', 'source__id'])
        .save('refs_named_sources_2')
    )

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 9. ANON REFS (again) ###')
    # --------------------------------------------------------------------

    # 9.0 do a second pass on anon refs to link some more refs to newfound sources. 
    # NOTE:  that no more sources can be discovered, or updated
    actions.set_legends(**{
        'sources': extr_sources.get(),
        'sources_updated': [],
        'sources_additional': []
    })
    actions.set_counters(*['cnt_existing', 'cnt_discovered', 'cnt_updated'])
    (extr_refs_anon
        .addattr('source__id', actions.s3__addattr__anonrefs__sourceid)
        .save('refs_anon_plus_sourcetitle_sourceid')
    )
    cntrs = actions.get_counters()
    log.info(f'-- Linked {cntrs["cnt_existing"]}/{len(extr_refs_anon.get())} anonrefs to existing sources.')
    log.info(f'-- Updated sources: {cntrs["cnt_updated"]}')         # should be 0
    # log.info(f'-- Discovered sources: {cntrs["cnt_discovered"]}') # doesn't matter

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 10. REFS (union named and non-named) ###')
    # --------------------------------------------------------------------

    # 10.0 join processed anon and named refs and create sourcelist containing source__ids
    # NOTE: at this point, source__title is no more needed for refs.
    extr_refs_anon.count('ANON REFS (refs_anon_src_1)')
    extr_refs_named.count('NAMED REFS (refs_named_src_3)')
    extr_refs = (extr_refs_named.fork()
        .extend(extr_refs_anon
            .addattr('sources', [])
            .addattr('complex', False)
            .mapto(actions.s3__mapto__refs__convert_srcid_srctitle_to_sourcelist)
            .remove_cols(['source__title', 'source__id'])
        )
        .save('refs_all_1')
    )

    # 10.1 identify void refs and separate them from non-void refs
    # a ref is void if it has no description.
    # >>> also transform event__id into a events list containing [event__id]
    extr_refs_void = (extr_refs.fork()
        .filter_rows(lambda ref: ref.desc is None)
        .save('refs_all_void')
    )
    extr_refs_nonvoid = (extr_refs.fork()
        .filter_rows(lambda ref: ref.desc is not None)
        .addattr('events', actions.s3__addattr__refs__create_events_list)
        .save('refs_all_nonvoid_0')
    )

    # 10.2 remove duplicate non-void refs by merging their event lists,
    # >>> the first encountered receives all the duplicates' event__ids in its events list.
    """
    Why do some non-void refs exist in multiple copies:
    When the same refname is used across multiple timeline pages, it is necessary to redefine it 
    (copying the text), so there is at least one non-void ref for that refname in that page.
    Duplicate non-void refs are unnecessary and it's safe to remove them by merging the event__ids.
    """
    actions.set_counters(*['cnt_found_duplicate_nonvoid'])
    actions.set_legends(**{
        'refs_nonvoid_0': extr_refs_nonvoid.get(),
        'refs_nonvoid_new_1': [],
    })
    (extr_refs_nonvoid
        .mapto(actions.s3__mapto__refs__merge_remove_duplicates_1)
    )
    cntrs = actions.get_counters()
    log.info(f'-- Found ({cntrs["cnt_found_duplicate_nonvoid"]}) duplicate non-void refs in refs_all_nonvoid_0.')
    log.info(f'-- Their event__ids have been added to the events list of the first occurrence in order of rid.')
    extr_refs_nonvoid = (Extractor(data=actions.get_legend('refs_nonvoid_new_1'))
        .sort()
        .select_cols(['rid', 'name', 'complex', 'events', 'sources', 'desc'])
        .save('refs_all_nonvoid_1')
    )

    # 10.3 merge void refs into nonvoids by appending the void ref's event__id in the non-void one.
    actions.set_counters(*['cnt_found_duplicate_void'])
    actions.set_legends(**{
        'refs_nonvoid_1': extr_refs_nonvoid.get(),
    })
    (extr_refs_void
        .mapto(actions.s3__mapto__refs__merge_remove_duplicates_2)
    )
    cntrs = actions.get_counters()
    log.info(f'-- Found ({cntrs["cnt_found_duplicate_void"]}) duplicate void refs in refs_all_void.')
    log.info(f'-- Their event__ids have been added to the events list of the first occurrence in order of rid.')
    
    # 10.4 update ref extractor (copy the content of refs_nonvoid_1)
    extr_refs = (Extractor(data=actions.get_legend('refs_nonvoid_1'))
        .sort()
        .save('refs_all_2')
    )

    # 10.5 filter out invalid refs, which then will be used for filtering invalid events
    extr_refs_invalid = (extr_refs
        .fork()
        .filter_rows(lambda ref: constants.TMP_MARKERS[1] in ref.sources)
        .save('refs_all_2_invalid')
    )
    (extr_refs
        .filter_rows(lambda ref: not constants.TMP_MARKERS[1] in ref.sources)
        .save('refs_all_3')
    )

    """
    COMPLEXITY PROBLEM #1
        The structure we're going for is the following:
            Event(*)--(*)Ref(*)--(*)Source
        Which gets complex when navigating from Sources to Events (since Sources will be the main lookup index).
        
        - By observing the N-N relationship between Ref and Source we see only 6 Refs to mention more than one Source, those
        are the multi-source refs found at [s3__mapto__namedrefs__add_srcid_multiple].
        - Those 6 Refs are linked to 1296 events, but only 1 of those events is linked to *only* this special Ref.

        To simplify the structure, we need to transform the N-N in a N-1 (each Ref only refers to a Source).
        - If we remove the multi-source refs, only 1 Event gets deleted (but we may lose information about placements and such)
        - Another option is to create a "special" source that aggregates the sources mentioned by the multi-source events, so
        we will add 6 new sources; but then, how do we access this information?
        - Third option is to remove the multi-source refs and mark the referred events with a special attribute, which will then be used
        to navigate to a "more info on the timeline placement"-style link, which is reasonable mainly because the six multi-source refs
        all have a very long description.
        So, option 3 suits the best.
    """

    # 10.6 filter out multi-source refs and restore a single "source" attribute for single-source refs (see COMPLEXITY PROBLEM #1)
    extr_refs_multisource = (extr_refs.fork()
        .filter_rows(lambda ref: len(ref.sources) > 1)
        .save('refs_all_3_multisource')
    )
    (extr_refs
        .filter_rows(lambda ref: len(ref.sources) == 1)
        .addattr('source', lambda ref: next(iter(ref.sources)))
        .remove_cols(['sources'])
        .save('refs_all_4')
    )

    """
    COMPLEXITY PROBLEM #2
        At this point we have the following structure:
            Event(*)--(*)Ref(*)--(1)Source
        Let us reconsider what is a Ref: a text linked to one or more Events in the timeline.
        Based on the Refs' names and descriptions, we were able to group Refs together by linking each Ref to (ONE) source.
        For this reason, a Ref is a link between an Event and a Source. 
        The problem is the following: an Event can be connected to the same Source by two or more Refs. Seen from the other side,
        A Source can reach an Event multiple times without knowing "how".
        
        At this point there are, conceptually, two types of Refs: 
        - A) Refs that add information to the link between Events and Sources
            Refs of type A are _usually_ complex refs (having a multi-token source name, like "GotGV2 Celestials").
        - B) Refs that add NO information to the link (e.g. have no description).
            Refs of type B are _usually_ non-complex refs (having a single-token source name, like "GotGV2").

        The proposal here is to simplify the link between Events and Sources making it such that there is only one path between
        every Source and Event.
        - In the domain of the visualization, there is no point in accessing Refs in an aggregate way.
        - There is also no point in having to keep refs which have no description. Refs of type B _usually_ have no description.
        
        We then introduce a new type of object straddling the N-N relationship between Refs and Events:
        - We promote type-B refs with description to type-A refs, this we can guarantee only one type-B ref exists for each source. To do so, we add a "noinfo" attribute. If noinfo=True, the ref is of type B.
        - For each type-B ref:
            - Add an entry on a m2m list with the following format:
                (sid, eid)
        - For each type-A ref:
            - If there is no entry on this list, add it
            - Create then a new object of type RefLink with the following attributes: eid, sid and rid.
        
        After this, update Sources, Events and Refs:
            - Link Events and Sources together
            - Links Events, Sources and Refs together using RefLinks (when needed)

        The final structure is the following:
                        Ref
                        (1)
                         ↑
                        (*)
        Event(1) → (*)Reflink(*) → (1)Source
            (*)                        (*)
             |                          | 
             ----------------------------
    """

    # 10.7 remove refs which have no sources linked
    (extr_refs
        .fork()
        .filter_rows(lambda ref: not ref.source)
        .save('refs_all_4_nosource')
     )
    (extr_refs
        .filter_rows(lambda ref: ref.source)
    )

    # 10.8 mark the type of ref based on the value of complex and the ref description.
    # >>> noinfo=True if ref is not complex AND ref.desc matches with patterns 1 or 3
    (extr_refs
        .addattr('noinfo', actions.s3__addattr__refs__noinfo, **{'patterns': [patterns[0], patterns[2]]})
        .save('refs_all_5')
    )

    # 10.9 merge non-complex refs which have the same source and same description
    (extr_refs
        .iterate(actions.s3__iterate__refs__merge_remove_duplicates_3)
        .save('refs_all_6')
    )
    
    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 11. EVENTS, REFS and SOURCES ###')
    # --------------------------------------------------------------------

    # 11.0 remove original refids from events, since duplicate refs were removed but non from here.
    (extr_events
        .remove_cols(['refs'])
        .save('events_1_refs_removed')
    )

    # 11.1 filter invalid events if their eid appears in one of the invalid refs' events list
    # among the refs filtered out at 9.5
    invalid_eids = (extr_refs_invalid
        .fork()
        .consume_key('events')
        .flatten().sort().unique()
        .get()
    )
    (extr_events
        .fork()
        .filter_rows(lambda ev: ev.eid in invalid_eids)
        .save('events_1_invalid_1')
    )
    (extr_events
        .filter_rows(lambda ev: not ev.eid in invalid_eids)
        .save('events_2')
    )

    # 11.2 update refs removing references to deleted events
    actions.set_legends(**{
        'invalid_eids': invalid_eids
    })
    actions.set_counters(*['cnt_updated'])
    (extr_refs
        .mapto(actions.s3__mapto__refs__clean_deleted_eids)
        .save('refs_all_7')
    )
    cntrs = actions.get_counters()
    log.info(f'-- Updated refs: {cntrs["cnt_updated"]}')


    # 11.3 filter orphan events if their id doesn't appear in any ref's events list
    actions.set_legends(**{
        'event2ref_map': {},
    })
    (extr_refs
        .fork()
        .mapto(actions.s3__mapto__refs__get_event2ref_map, **{'name': 'event2ref_map'})
    )
    (extr_events
        .fork()
        .filter_rows(lambda ev: ev.eid not in actions.get_legend('event2ref_map').keys())
        .save('events_2_invalid_2')
    )
    (extr_events
        .filter_rows(lambda ev: ev.eid in actions.get_legend('event2ref_map').keys())
    )

    # 11.4 iterate refs and build the m2m and reflink lists
    actions.set_legends(**{
        'sources2events_m2m': [],
        'reflinks': [],
    })
    (extr_refs
        .mapto(actions.s3__mapto__refs__get_sources2events_m2m)
    )
    extr_m2m = (Extractor(data=actions.get_legend('sources2events_m2m'))
        .sort()
        .save('m2m', nostep=True)
    )
    extr_reflinks = (Extractor(data=actions.get_legend('reflinks'))
        .save('final_reflinks', nostep=True)
    )

    # 11.5 link Events with Sources and Reflinks (see COMPLEXITY PROBLEM #2).
    reflinks_by_evt = (extr_reflinks.fork()
        .sort('evt').groupby('evt')
        .get()
    )
    actions.set_legends(**{
        'sources2events_m2m': extr_m2m.get(),
        'reflinks_by_evt': reflinks_by_evt
    })
    actions.set_counters(*['cnt_existing'])
    (extr_events
        .addattr('sources', actions.s3__addattr__events__sources_list)
        .addattr('reflinks', actions.s3__addattr__events__reflinks_list)
    )
    cntrs = actions.get_counters()
    log.info(f'-- Linked {cntrs["cnt_existing"]}/{len(extr_events.get())} events to reflinks.')

    # 11.6 add a ref_special attribute to events which where linked to removed multi-source refs (see COMPLEXITY PROBLEM #1).
    # >>> the attribute contains the multi-source ref ID.
    actions.set_legends(**{
        'event2multisrcref_map': {},
    })
    (extr_refs_multisource.fork()
        .mapto(actions.s3__mapto__refs__get_event2ref_map, **{'name': 'event2multisrcref_map'})
    )
    (extr_events
        .addattr('ref_special', actions.s3__addattr__events__special_multisource_ref_id, **{'name': 'event2multisrcref_map'})
        .save('events_3')
    )

    # 11.7 at this point, we can filter refs which have noinfo=True, since they won't be used for linking anymore
    (extr_refs
        .fork()
        .filter_rows(lambda ref: ref.noinfo)
        .save('refs_all_7_noinfo')
    )
    (extr_refs
        .filter_rows(lambda ref: not ref.noinfo)
        .save('refs_all_8')
    )

    # 11.8 add ref ids to sources
    # >>> only refs with noinfo=False are added
    actions.set_legends(**{
        'refs': extr_refs.get(),
    })
    (extr_sources
        .addattr('refs', actions.s3__addattr__sources__refids)  
    )

    # 11.9 link Sources with Events and Reflinks (see COMPLEXITY PROBLEM #2).
    """
    We maintain both ends of the relationship to support both import scenarios (we don't know which to use yet)
    """
    reflinks_by_src = (extr_reflinks.fork()
        .sort('src').groupby('src')
        .get()
    )
    actions.set_legends(**{
        'sources2events_m2m': extr_m2m.get(),
        'reflinks_by_src': reflinks_by_src
    })
    actions.set_counters(*['cnt_existing_in_m2m', 'cnt_existing_in_reflinks'])
    (extr_sources
        .addattr('events', actions.s3__addattr__sources__events_list)
        .addattr('reflinks', actions.s3__addattr__sources__reflinks_list)
        .save('sources_8')
    )
    cntrs = actions.get_counters()
    log.info(f'-- Linked {cntrs["cnt_existing_in_m2m"]}/{len(extr_sources.get())} sources to events.')
    log.info(f'\t\t Out of these sources, {cntrs["cnt_existing_in_reflinks"]} go through reflinks.')

    # 11.10 remove duplicate Sources 
    """
    It's perfectly OK to have duplicate sources and to handle them now, since they are exact copies 
    (having the same sid) with the exception of one having the title and one not having it.
    """
    (extr_sources
        .iterate(actions.s3__iterate__sources__merge_remove_duplicates)
        .save('sources_9')
    )

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 13. SOURCE HIERARCHY ###')
    # --------------------------------------------------------------------

    # 13.1 build source/ref hierarchy by adding sub_sources recursive list.
    actions.set_legends(**{'hierarchy': []})
    (extr_sources
        .fork()
        .addattr('sub_sources', [])
        .addattr('level', actions.s3__addattr__sources__level)
        .mapto(actions.s3__mapto__sources__hierarchy_level0)
        .mapto(actions.s3__mapto__sources__hierarchy_level1)
        .mapto(actions.s3__mapto__sources__hierarchy_level2)
        .save('sources_level')
    )

    # 13.2 obtain non-flattened 3-level file with all refs (referenced by ID),
    # grouped by main source, subgrouped by (eventually) seasons and episodes or by films.
    extr_hierarchy = (Extractor(data=actions.get_legend('hierarchy'))
        .save('timeline_hierarchy')
    )

    # ---------------------------------------------------------------------------------

    # 13.3A hierarchy for tv shows
    (extr_hierarchy.fork()
        .filter_rows(lambda rootsrc: rootsrc.type == constants.SRC_TV_SERIES)
        .save('timeline_hierarchy_tv')
    )

    # 13.3B hierarchy for films
    film_countrefs=(extr_hierarchy.fork()
        .filter_rows(lambda rootsrc: rootsrc.type == constants.SRC_FILM_SERIES)
        .save('timeline_hierarchy_film')
        .consume_key('sub_sources')
        .flatten()
        .addattr('year', lambda src: src.details['year'])
        .save('timeline_hierarchy_film_countrefs')
        .get()
    )

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 14. FINALIZE ###')
    # --------------------------------------------------------------------

    log.info('Finalizing output without step number:')
    (extr_sources.save('final_sources', nostep=True))
    (extr_refs.save('final_refs', nostep=True))
    (extr_events.save('final_events', nostep=True))
    
    (extr_refs_multisource.save('extra_refs_multisource', nostep=True))

    log.info(f'### End ###')

if __name__ == "__main__":
    logconfig.config()
    main()
