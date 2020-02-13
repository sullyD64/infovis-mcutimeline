# s3_extract.py
import logging as log

from py_logic.actions import Actions
from py_logic.extractor import Extractor
from py_utils.constants import TMP_MARKERS, SRC_FILM_SERIES, SRC_TV_SERIES
from py_utils.errors import RequiredInputMissingError
from settings import INPUT_MANUAL, OUTPUT

CODE = 's3'
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
        raise RequiredInputMissingError(CODE)

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
        .iterate(actions.s3__iterate__sources__parse_raw)
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
        .mapto(actions.s3__mapto__events__parse_raw)
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
        .addattr('source__title', actions.s3__addattr__refs__sourcetitle, use_element=True, **{'patterns': patterns})
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
        .addattr('source__id', actions.s3__addattr__anonrefs__sourceid, use_element=True)
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
        .addattr('source__title', actions.s3__addattr__refs__sourcetitle, use_element=True, **{'patterns': patterns})
        .save('refs_named_plus_sourcetitle')
    )
    cntrs = actions.get_counters()
    log.info(f'-- Added {cntrs["cnt_existing"]}/{len(extr_refs_named.get())} sourcetitles to namedrefs.')
    log.info(f'-- Updated refnames: {cntrs["cnt_updated"]}')

    # 5.3 link ref to sources based on inferred sourcetitle, refname and the source's title and id
    # >>> if sourcetitle matches with existing source but it has no id, the source is updated with the new id based on the refname.
    # >>> if no source is found either using sourcetitle or refname, a new source is discovered and added.
    # IMPORTANT: two passes are necessary, because of the intrinsic asymmetricality:

    # 5.3.1 first pass (perform positive matches and updates)
    log.info(f'** first pass **')
    actions.set_legends(**{
        'sources': extr_sources.get(),
        'sources_updated': [],
        'sources_additional': [],
    })
    actions.set_counters(*['cnt_existing', 'cnt_discovered', 'cnt_updated', 'cnt_ref_marked'])
    (extr_refs_named
        .addattr('source__id', actions.s3__addattr__namedrefs__sourceid_1, use_element=True)
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
        .addattr('source__id', actions.s3__addattr__namedrefs__sourceid_2, use_element=True)
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

    # 6 merge sources after update and discover
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

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 8. NAMED REFS (again) ###')
    # --------------------------------------------------------------------

    # 8.1 begin adding sources as lists, starting from "multiple" refnames
    actions.set_legends(**{'sources': extr_sources.get()})
    actions.set_counters(*['cnt__secondary_ref_found', 'cnt__not_a_source'])
    (extr_refs_named
        .addattr('sources', [])
        .mapto(actions.s3__mapto__namedrefs__add_srcid_multiple)
        .addattr('source__title', actions.s3__addattr__namedrefs__remaining_srctitle, use_element=True)
        .save('refs_named_sources_1')
    )
    cnt_tot = len(extr_refs_named.get())
    cntrs = actions.get_counters()
    log.info(f'-- Out of {cnt_tot} named refs:')
    log.info(f'\t-- {cnt_tot - cntrs["cnt__secondary_ref_found"] - cntrs["cnt__not_a_source"]} refs were left unchanged.')
    log.info(f'\t-- {cntrs["cnt__secondary_ref_found"]} refs have a complex name (multiple tokens) but refer to a valid source, so they are secondary refs.')
    log.info(f'\t\tThose are marked with {TMP_MARKERS[2]} and will have is_secondary=True')
    log.info(f'\t-- {cntrs["cnt__not_a_source"]} refs have a complex name (multiple tokens) which is invalid (not a source)')
    log.info(f'\t\tThose are marked with {TMP_MARKERS[1]} and will be removed.')

    # 8.2 finish converting source ids into lists and cleanup
    # >>> add is_secondary to the refs w/ multiple tokens but referring to a valid source.
    (extr_refs_named
        .addattr('is_secondary', actions.s3__addattr__namedrefs__is_secondary, use_element=True)
    )

    ##############################################
    #   TODO don't delete refs with complex names which aren't sources, we need them
    #   for identifying invalid events.
    ##############################################

    (extr_refs_named
        .fork()
        .filter_rows(lambda ref: ref.source__id == TMP_MARKERS[1])
        .consume_key('name')
        .unique()
        .sort()
        .save('ref_invalide', nostep=True)
    )
    
    (extr_refs_named
        .filter_rows(lambda ref: ref.source__id != TMP_MARKERS[1])
        .mapto(actions.s3__mapto__refs__convert_srcid_srctitle_to_sourcelist)
        .remove_cols(['source__title', 'source__id'])
        .save('refs_named_sources_2')
    )

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 9. REFS (union named and non-named) ###')
    # --------------------------------------------------------------------

    # 9.0 join processed anon and named refs and create sourcelist containing source__ids
    # NOTE: at this point, source__title is no more needed for refs.
    extr_refs_anon.count('ANON REFS (refs_anon_src_1)')
    extr_refs_named.count('NAMED REFS (refs_named_src_3)')
    extr_refs = (extr_refs_named.fork()
        .extend(extr_refs_anon
            .addattr('sources', [])
            .addattr('is_secondary', False)
            .mapto(actions.s3__mapto__refs__convert_srcid_srctitle_to_sourcelist)
            .remove_cols(['source__title', 'source__id'])
        )
        .save('refs_all_1')
    )

    # 9.1 identify void refs and separate them from non-void refs
    # a ref is void if it has no description.
    # >>> also transform event__id into a events list containing [event__id]
    extr_refs_void = (extr_refs.fork()
        .filter_rows(lambda ref: ref.desc is None)
        .save('refs_all_void')
    )
    extr_refs_nonvoid = (extr_refs.fork()
        .filter_rows(lambda ref: ref.desc is not None)
        .addattr('events', actions.s3__addattr__refs__create_events_list, use_element=True)
        .save('refs_all_nonvoid_0')
    )

    # 9.2 remove duplicate non-void refs by merging their event lists, 
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
        .select_cols(['rid', 'name', 'is_secondary', 'events', 'sources', 'desc'])
        .save('refs_all_nonvoid_1')
    )

    # 9.3 merge void refs into nonvoids by appending the void ref's event__id in the non-void one.
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
    
    # 9.4 update ref extractor
    extr_refs = (Extractor(data=actions.get_legend('refs_nonvoid_1'))
        .sort()
        .save('refs_all_2')
    )

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 10. SOURCE HIERARCHY ###')
    # --------------------------------------------------------------------

    # 10.0 add refids and ref count in a list to each source
    actions.set_legends(**{
        'refs_primary': extr_refs.fork().filter_rows(lambda ref: not ref.is_secondary).get(),
        'refs_secondary': extr_refs.fork().filter_rows(lambda ref: ref.is_secondary).get()
    })
    (extr_sources
        .addattr('refs_primary', actions.s3__addattr__sources__refids, use_element=True, **{'type': 'primary'})
        .addattr('refs_primary_count', lambda src: len(src.refs_primary), use_element=True)
        .addattr('refs_secondary', actions.s3__addattr__sources__refids, use_element=True, **{'type': 'secondary'})
        .addattr('refs_secondary_count', lambda src: len(src.refs_secondary), use_element=True)
        .addattr('refs_tot_count', lambda src: src.refs_primary_count + src.refs_secondary_count, use_element=True)
        .save('sources_refs')
    )

    # 10.1 build source/ref hierarchy by adding sub_sources recursive list.
    actions.set_legends(**{'hierarchy': []})
    (extr_sources
        .addattr('sub_sources', [])
        .addattr('level', actions.s3__addattr__sources__level, use_element=True)
        .mapto(actions.s3__mapto__sources__hierarchy_level0)
        .mapto(actions.s3__mapto__sources__hierarchy_level1)
        .mapto(actions.s3__mapto__sources__hierarchy_level2)
        .save('sources_level')
    )

    # 10.2 obtain final, non-flattened, 3-level file with all refs (referenced by ID),
    # grouped by main source, subgrouped by (eventually) seasons and episodes or by films.
    extr_hierarchy = (Extractor(data=actions.get_legend('hierarchy'))
        .save('timeline_hierarchy')
    )

    # ---------------------------------------------------------------------------------

    # 10.3A hierarchy for tv shows
    extr_tv = (extr_hierarchy.fork()
        .filter_rows(lambda rootsrc: rootsrc.type == SRC_TV_SERIES)
        .save('timeline_hierarchy_tv')
    )

    # 10.3B hierarchy for films
    extr_films = (extr_hierarchy.fork()
        .filter_rows(lambda rootsrc: rootsrc.type == SRC_FILM_SERIES)
        .save('timeline_hierarchy_film')
    )

    film_countrefs = (extr_films
        .consume_key('sub_sources')
        .flatten()
        .addattr('year', lambda src: src.details['year'], use_element=True)
        .select_cols(['year', 'title', 'refs_primary_count', 'refs_secondary_count', 'refs_tot_count'])
        .save('timeline_hierarchy_film_countrefs')
        .get()
    )
    log.info('')
    log.info('-- Unique ref count: ')
    for src in sorted(film_countrefs, key=lambda src: src.year):
        log.info(f'{src.year}, {src.title}, primary: {src.refs_primary_count}, secondary: {src.refs_secondary_count}, tot: {src.refs_tot_count}')

    # --------------------------------------------------------------------
    log.info('')
    log.info(f'### 11. EVENTS ###')
    # --------------------------------------------------------------------

    # 11.0 remove original refids from events, since duplicate refs were removed but non from here.
    (extr_events
        # .addattr('refs', lambda event: [ref.rid for ref in event.refs], use_element=True)
        .remove_cols(['refs'])
        .count('events_stripped')
        .save('events_stripped')
    )

    log.info(f'### End ###')

if __name__ == "__main__":
    main()
