import glob
import os
import re

from logic import Extractor, ExtractorActions
from structs import Event
from const import TMP_MARKERS, SRC_TYPES

DIR = os.path.dirname(__file__)
OUT_DIR = os.path.join(DIR, 'auto')

NAMEDREFS_INCLUDE_VOID = False
NAMEDREFS_INCLUDE_VOID = True

if __name__ == "__main__":
    for outfile in glob.glob(f'{OUT_DIR}/extracted__*'):
        os.remove(outfile)

    actions = ExtractorActions()

# =============================
# 1. SOURCES
# =============================
    print(f'\nSOURCES')

    manual_dir = os.path.join(DIR, 'manual')
    actions.set_legends(**{'series_seasons': []})

    # extract sources by combining films and tv episodes
    extr_sources = (Extractor(f'{manual_dir}/films.json')
        .addattr('title', actions.sources__add_attrs, use_element=True, **{'to_add': 't'})
        .addattr('details', actions.sources__add_attrs, use_element=True, **{'to_add': 'd'})
        .addattr('sid', actions.sources__add_attrs, use_element=True, **{'to_add': 's'})
        .addattr('type', SRC_TYPES['film'])
        .select_cols(['sid', 'title', 'type', 'details'])
        .extend(Extractor(f'{manual_dir}/tv_episodes.json')
            .addattr('title', actions.sources__add_attrs, use_element=True, **{'to_add': 't'})
            .addattr('details', actions.sources__add_attrs, use_element=True, **{'to_add': 'd'})
            .addattr('sid', actions.sources__add_attrs, use_element=True, **{'to_add': 's'})
            .addattr('type', SRC_TYPES['tv_episode'])
            .select_cols(['sid', 'title', 'type', 'details'])
            .mapto(actions.mapto__sources__extend_episode_series_and_season)
            .extend(
                Extractor(data=actions.get_legend('series_seasons'))
            )
        )
        .count('sources_0')
        .save('sources_0')
    )

    print('='*100)
# =============================
# 2. REFS
# =============================
    print(f'\nREFS')

    infile_parsed = os.path.join(DIR, f'{OUT_DIR}/parsed.json')
    extr_events = (Extractor(infile_parsed)
        .mapto(lambda ev: Event.from_dict(**ev))  # Parse raw data into Event and Ref objects.
        .count('events')
        .save('events')
    )

    # extract all refs, flattened
    extr_refs = (extr_events.fork()
        .consume_key('refs')
        .flatten()
        .remove_cols(['links'])
        .count('refs_all_0')
        .save('refs_all_0')
    )

    print('='*100)
# =============================
# 2.1 ANONYMOUS REFS
# =============================
    print(f'\nANONYMOUS REFS')

    # extract anonymous refs
    extr_refs_anon = (extr_refs.fork()
        .filter_rows(lambda ref: ref.name is None)
        .unique()
        .sort()
        .count('refs_anon')
        .save('refs_anon')
    )

    # TODO consider if including $ or not for anon refs (include Deleted Scenes, etc.)
    pattern__anon__begin_title_end = r'^<i>([^\<\>]*)</i>$'
    pattern__anon__begin_in_title_continue = r'In <i>([^\<\>]*)</i>'

    # step 0: extract valid anonymous refs, then adds source title
    # filters out invalid refs, for which it hasn't been found a proper source title and thus would be impossible to add a source id later on.
    extr_refs_anon = (extr_refs_anon.fork()
        .filter_rows(lambda ref: re.match(pattern__anon__begin_title_end, ref.desc))
        .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__anon__begin_title_end})
        .extend(
            extr_refs_anon.fork()
            .filter_rows(lambda ref: re.match(pattern__anon__begin_in_title_continue, ref.desc))
            .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__anon__begin_in_title_continue})
        )
        .count('refs_anon_src_0')
        .save('refs_anon_src_0')
    )

    actions.set_legends(**{
        'sources': extr_sources.get(),
        'sources_missing': []
    })
    actions.set_counters(*['cnt_found', 'cnt_notfound', 'cnt_sources_updated'])

    # step 1: add source id using source title as reference
    (extr_refs_anon
        .addattr('source__id', actions.anonrefs__add_srcid, use_element=True)
        .count('refs_anon_src_1')
        .save('refs_anon_src_1')
    )

    cnt_tot = len(extr_refs_anon.get())
    cntrs = actions.get_counters()
    print(f'[anonrefs__add_srcid]: sources added to [{cntrs["cnt_found"]}/{cnt_tot}] anon refs (not found: {cntrs["cnt_notfound"]})')

    print('='*100)
# =============================
# 2.2 Update sources
# =============================
    print(f'\nUPDATE SOURCES (after anonrefs__add_srcid, the title of some sources has been modified)')

    sources_missing = actions.get_legend('sources_missing')
    (Extractor(data=sources_missing)
        .count('sources_missing_1')
        .save('sources_missing_1')
    )

    print(f'missing sources: added {cntrs["cnt_notfound"]} new sources.')

    extr_sources = (Extractor(data=actions.get_legend('sources'))
        .count('sources_1')
        .save('sources_1')
    )

    actions.set_legends(**{
        'sources': extr_sources.get(),
        'sources_missing': sources_missing
    })
    print(f'sources: updated {cntrs["cnt_sources_updated"]} titles.')

    print('='*100)
# =============================
# 2.3 NAMED REFS
# =============================
    print(f'\nNAMED REFS')

    # extract named refs
    extr_refs_named = (extr_refs.fork()
        .filter_rows(lambda ref: ref.name is not None)
        .sort()
        .count('refs_named')
        .save('refs_named')
    )

    # extract unique named refs
    """
    Working on unique when coding the job is the same: unique means "with same name and desc".
    De facto, each ref is unique on its own, since it refers to a different event,
    but refs in a void html tag are just pointers to non-void ref tags, which are far more relevant.
    """
    extr_refs_named_unique = (extr_refs_named.fork()
        .filter_rows(lambda ref: ref.desc is not None)
        .unique()
        .sort()
        .count('refs_named_unique')
        .save('refs_named_unique')
    )

    # ------------------------------------------------------
    # MODE SWITCHER between unique-only and all named refs
    if not NAMEDREFS_INCLUDE_VOID:
        print("** WARNING: Using unique, non-void named refs **")
        extr_refs_named = extr_refs_named_unique
    # ------------------------------------------------------

    pattern__named__begin_title_end = r'^<i>([^\<\>]*)</i>$'
    pattern__named__begin_in_title_continue = r'^In <i>([^\<\>]*)</i>'

    # step 0: add source title
    (extr_refs_named
        .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__named__begin_title_end})
        .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__named__begin_in_title_continue})
        .count('refs_named_src_0')
        .save('refs_named_src_0')
    )

    actions.set_counters(*['cnt__matching_exact', 'cnt__sources_missing_updated'])
    cnt_tot = len(extr_refs_named.get())

    # step 1: add source id using source title as reference
    (extr_refs_named
        .addattr('source__id', actions.namedrefs__add_srcid, use_element=True)
        .count('refs_named_src_1')
        .save('refs_named_src_1')
    )

    cntrs = actions.get_counters()
    print(
        f'[namedrefs__add_srcid]: out of {cnt_tot} named refs:\n'
        f'\t- {cntrs["cnt__matching_exact"]} refs have a name that matches exactly with an existing source id.\n'
        f'\t- {cntrs["cnt__sources_missing_updated"]} refs have a valid name (one token) but not present existing sources, so we add a new source in sources_missing.\n'
        f'\t- {cnt_tot - cntrs["cnt__matching_exact"] - cntrs["cnt__sources_missing_updated"]} refs have a complex name (multiple tokens) and require further analysis.'
    )

    # extra: create a unique refnames list
    refnames = (extr_refs_named.fork()
        .consume_key('name')
        .unique()
        .sort()
        .count('refnames_legend')
        .save('refnames_legend')
        .get()
    )

    print('='*100)
# =============================
# 2.4 Update sources
# =============================
    print(f'\nUPDATE SOURCES (after namedrefs__add_srcid, some missing sources were added)')

    # extend updated sources with updated missing sources
    (extr_sources
        .extend(
            Extractor(data=actions.get_legend('sources_missing'))
            .count('sources_missing_2')
            .save('sources_missing_2')
        )
        .count('sources_2')
        .save('sources_2')
    )

    actions.set_legends(**{
        'sources': extr_sources.get()
    })

    print(f'missing sources: added {cntrs["cnt__sources_missing_updated"]} new sources.')
    print(f'sources: combined sources with missing sources, now available as a single list of {len(extr_sources.get())} sources.')

    print('='*100)
# =============================
# 2.4 NAMED REFS again
# =============================
    print(f'\nNAMED REFS')

    actions.set_counters(*['cnt__secondary_ref_found', 'cnt__not_a_source'])

    # step 2: begin adding sources as lists, starting from "multiple" refnames
    (extr_refs_named
        .addattr('sources', [])
        .mapto(actions.mapto__namedrefs__add_srcid_multiple)
        .addattr('source__title', actions.namedrefs__add_missing_srctitle, use_element=True)
        .count('refs_named_src_2')
        .save('refs_named_src_2')
    )

    cntrs = actions.get_counters()
    print(
        f'[mapto__namedrefs__add_srcid_multiple]: out of {cnt_tot} named refs:\n'
        f'\t- {cnt_tot - cntrs["cnt__secondary_ref_found"] - cntrs["cnt__not_a_source"]} refs were left unchanged.\n'
        f'\t- {cntrs["cnt__secondary_ref_found"]} refs have a complex name (multiple tokens) but refer to a valid source, so they are secondary refs.\n'
        f'\t\tThose are marked with {TMP_MARKERS[2]} and will have is_secondary=True\n'
        f'\t- {cntrs["cnt__not_a_source"]} refs have a complex name (multiple tokens) which is invalid (not a source)\n'
        f'\t\tThose are marked with {TMP_MARKERS[1]} and will be removed.'
    )

    # step 3: finish converting source ids into lists and cleanup
    (extr_refs_named
        .addattr('is_secondary', lambda **kwargs: kwargs['element'].source__id == TMP_MARKERS[2], use_element=True)
        .filter_rows(lambda ref: ref.source__id != TMP_MARKERS[1])
        .mapto(actions.mapto__refs__convert_srcid_srctitle_to_sourcelist)
        .remove_cols(['source__title', 'source__id'])
        .count('refs_named_src_3')
        .save('refs_named_src_3')
    )

    print('='*100)
# =============================
# 2.5 REFS, union between named and non-named
# =============================
    print(f'\nREFS: ANON + NAMED')

    extr_refs_anon.count('refs_anon_src_1')
    extr_refs_named.count('refs_named_src_3')
    extr_refs = (extr_refs_named.fork()
        .extend(extr_refs_anon
            .addattr('sources', [])
            .addattr('is_secondary', False)
            .mapto(actions.mapto__refs__convert_srcid_srctitle_to_sourcelist)
            .remove_cols(['source__title', 'source__id'])
        )
        .count('refs_all_1')
        .save('refs_all_1')
    )

    # step 0: separate void refs from non-void and transform event__id into a events list containing [event__id]
    extr_refs_void = (extr_refs.fork()
        .filter_rows(lambda ref: ref.desc is None)
        .count('refs_all_void')
        .save('refs_all_void')
    )
    extr_refs_nonvoid = (extr_refs.fork()
        .filter_rows(lambda ref: ref.desc is not None)
        .addattr('events', lambda **kwargs: [kwargs['element'].event__id], use_element=True)
        .count('refs_all_nonvoid_0')
        .save('refs_all_nonvoid_0')
    )

    # step 1: remove duplicate non-void refs by merging their event lists, the first encountered receives all the duplicates' event__ids.
    """
    The reason some non-void refs exist in multiple copies is because this:
    When the same refname is used across multiple timeline pages, it is necessary to redefine it (copying the text), 
    so there is at least one non-void ref for that refname in that page.
    Duplicate non-void refs are unnecessary and it's safe to remove them by merging the event__ids. 
    """
    actions.set_counters(*['cnt_found_duplicate_nonvoid'])
    actions.set_legends(**{
        'refs_nonvoid_0': extr_refs_nonvoid.get(),
        'refs_nonvoid_new_1': [],
    })
    (extr_refs_nonvoid
        .mapto(actions.mapto__refs__remove_duplicates_1)
    )
    cntrs = actions.get_counters()
    print(
        f'[mapto__refs__remove_duplicates_1]: found ({cntrs["cnt_found_duplicate_nonvoid"]}) duplicate non-void refs in refs_all_nonvoid_0.\n'
        f'\tTheir event__ids have been added to the events list of the first occurrence in order of rid.'
    )
    extr_refs_nonvoid = (Extractor(data=actions.get_legend('refs_nonvoid_new_1'))
        .sort()
        .select_cols(['rid', 'name', 'is_secondary', 'events', 'sources', 'desc'])
        .count('refs_all_nonvoid_1')
        .save('refs_all_nonvoid_1')
    )

    # step 2: merge void refs into nonvoids by appending the void ref's event__id in the non-void one.
    actions.set_counters(*['cnt_found_duplicate_void'])
    actions.set_legends(**{
        'refs_nonvoid_1': extr_refs_nonvoid.get(),
    })
    (extr_refs_void
        .mapto(actions.mapto__refs__remove_duplicates_2)
    )
    cntrs = actions.get_counters()
    print(
        f'[mapto__refs__remove_duplicates_2]: found ({cntrs["cnt_found_duplicate_void"]}) duplicate void refs in refs_all_void.\n'
        f'\tTheir event__ids have been added to the events list of the first occurrence in order of rid.'
    )
    extr_refs = (Extractor(data=actions.get_legend('refs_nonvoid_1'))
        .sort()
        .count('refs_all_2')
        .save('refs_all_2')
    )

    print('='*100)
# =============================
# 3 HIERARCHY, final
# =============================
    print(f'\nHIERARCHY')

    # add film series root sources
    actions.set_legends(**{'series_films': []})
    (extr_sources
        .mapto(actions.mapto__sources__extend_film_series)
        .extend(Extractor(data=actions.get_legend('series_films')))
    )

    # add ref ids and ref count in a list to each source
    actions.set_legends(**{
        'refs_primary': extr_refs.fork().filter_rows(lambda ref: not ref.is_secondary).get(),
        'refs_secondary': extr_refs.fork().filter_rows(lambda ref: ref.is_secondary).get()
    })
    (extr_sources
        .addattr('refs_primary', actions.sources__add_refs, use_element=True, **{'type': 'primary'})
        .addattr('refs_primary_count', lambda **kwargs: len(kwargs['element']['refs_primary']), use_element=True)
        .addattr('refs_secondary', actions.sources__add_refs, use_element=True, **{'type': 'secondary'})
        .addattr('refs_secondary_count', lambda **kwargs: len(kwargs['element']['refs_secondary']), use_element=True)
        .addattr('refs_tot_count', lambda **kwargs: kwargs['element']['refs_primary_count'] + kwargs['element']['refs_secondary_count'], use_element=True)
        .count('sources_3_refs')
        .save('sources_3_refs')
    )

    # build source/ref hierarchy
    actions.set_legends(**{'hierarchy': []})
    (extr_sources
        .addattr('sub_sources', [])
        .addattr('level', actions.sources__mark_level, use_element=True)
        .mapto(actions.mapto__sources__hierarchy_level0)
        .mapto(actions.mapto__sources__hierarchy_level1)
        .mapto(actions.mapto__sources__hierarchy_level2)
        .count('sources_4_level')
        .save('sources_4_level')
    )

    # obtain final, non-flattened, 3-level file with all refs (referenced by ID),
    # grouped by main source, subgrouped by (eventually) seasons and episodes or by films.
    extr_hierarchy = (Extractor(data=actions.get_legend('hierarchy'))
        .count('timeline_hierarchy')
        .save('timeline_hierarchy')
    )

    # hierarchy for tv shows
    extr_tv = (extr_hierarchy.fork()
        .filter_rows(lambda root: root['type'] == SRC_TYPES['tv_series'])
        .count('timeline_hierarchy_tv')
        .save('timeline_hierarchy_tv')
    )

    # hierarchy for films
    extr_films = (extr_hierarchy.fork()
        .filter_rows(lambda root: root['type'] == SRC_TYPES['film_series'])
        .count('timeline_hierarchy_film')
        .save('timeline_hierarchy_film')
    )

    # ---------------------------------------------------------------------------------

    film_countrefs = (extr_films
        .consume_key('sub_sources')
        .flatten()
        .addattr('year', lambda **kwargs: kwargs['element']['details']['year'], use_element=True)
        .select_cols(['year', 'title', 'refs_primary_count', 'refs_secondary_count', 'refs_tot_count'])
        .save('timeline_hierarchy_film_countrefs')
        .get()
    )

    film_countrefs = [f'{d["year"]}, {d["title"]}, {d["refs_primary_count"]}, {d["refs_secondary_count"]}, {d["refs_tot_count"]}' for d in film_countrefs]
    print()
    for frc in film_countrefs:
        print(frc)

    print('='*100)
# =============================
# 4 EVENTS
# =============================
    print(f'\nEVENTS')

    # remove original refids from events, since they were de-duplicated and they are now unaligned.
    (extr_events
        # .addattr('refs', lambda **kwargs: [ref.rid for ref in kwargs['element'].refs], use_element=True)
        .remove_cols(['refs'])
        .count('events_stripped')
        .save('events_stripped')
    )
