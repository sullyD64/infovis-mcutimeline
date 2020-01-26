import glob
import os
import re

from logic import Extractor, ExtractorActions
from structs import Event
from utils import TMP_MARKER_1, TMP_MARKER_2

DIR = os.path.dirname(__file__)
OUT_DIR = os.path.join(DIR, 'auto')


if __name__ == "__main__":
    for outfile in glob.glob(f'{OUT_DIR}/extracted__*'):
        os.remove(outfile)

    actions = ExtractorActions()

# =============================
# 1. SOURCES
# =============================
    print(f'\nSOURCES')

    manual_dir = os.path.join(DIR, 'manual')
    extr_movies = Extractor(f'{manual_dir}/movies.json')
    extr_episodes = Extractor(f'{manual_dir}/episodes.json')

    actions.set_legends(**{'series_seasons':  []})
    # extract sources by combining movies and tv episodes
    extr_sources = (extr_movies.fork()
        .addattr('title', actions.source__add_attrs, use_element=True, **{'to_add': 't'})
        .addattr('details', actions.source__add_attrs, use_element=True, **{'to_add': 'd'})
        .addattr('sid', actions.source__add_attrs, use_element=True, **{'to_add': 's'})
        .addattr('type', 'film')
        .filter_cols(['sid', 'title', 'type', 'details'])
        .extend(extr_episodes
            .addattr('title', actions.source__add_attrs, use_element=True, **{'to_add': 't'})
            .addattr('details', actions.source__add_attrs, use_element=True, **{'to_add': 'd'})
            .addattr('sid', actions.source__add_attrs, use_element=True, **{'to_add': 's'})
            .addattr('type', 'episode')
            .filter_cols(['sid', 'title', 'type', 'details'])
            .mapto(actions.mapto__source__extend_series_season)
            .extend(
                Extractor(data=actions.get_legend('series_seasons'))
            )
        )
        .count('sources')
        .save('sources')
    )

    print('='*100)
# =============================
# 2. REFS
# =============================
    print(f'\nREFS')

    infile_parsed = os.path.join(DIR, f'{OUT_DIR}/parsed.json')
    extr_refs = Extractor(infile_parsed)

    # extract all refs, flattened
    (extr_refs
        .mapto(lambda ev: Event.from_dict(**ev))  # Parse raw data into Event and Ref objects.
        .save('events')
        .consume_key('refs')
        .flatten()
        .filter_cols(['rid', 'event__id', 'name', 'desc'])
        .count('refs')
        .save('refs')
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

    # extracts valid anonymous refs, then adds source title
    # filters out invalid refs, for which it hasn't been found a proper source title and thus would be impossible to add a source id later on.
    extr_refs_anon = (extr_refs_anon.fork()
        .filter_rows(lambda ref: re.match(pattern__anon__begin_title_end, ref.desc))
        .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__anon__begin_title_end})
        .extend(
            extr_refs_anon.fork()
            .filter_rows(lambda ref: re.match(pattern__anon__begin_in_title_continue, ref.desc))
            .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__anon__begin_in_title_continue})
        )
        .count('refs_anon_srctitle')
        .save('refs_anon_srctitle')
    )

    actions.set_legends(**{
        'sources': extr_sources.get(), 
        'sources_missing': []
    })
    actions.set_counters(*['cnt_found', 'cnt_notfound', 'cnt_updated_sources'])
    cnt_tot = len(extr_refs_anon.get())

    (extr_refs_anon
        .addattr('source__id', actions.anonrefs__add_srcid, use_element=True)
        .save('refs_anon_srctitle_srcid')
    )

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

    updated_sources = actions.get_legend('sources')
    extr_updated_sources = (Extractor(data=updated_sources)
        .count('sources_updated_1')
        .save('sources_updated_1')
    )

    actions.set_legends(**{
        'sources': updated_sources, 
        'sources_missing': sources_missing
    })
    print(f'sources: updated {cntrs["cnt_updated_sources"]} titles.')

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
    extr_refs_named_unique = (extr_refs_named.fork()
        .unique()
        .sort()
        .count('refs_named_unique')
        .save('refs_named_unique')
    )

    pattern__named__begin_title_end = r'^<i>([^\<\>]*)</i>$'
    pattern__named__begin_in_title_continue = r'^In <i>([^\<\>]*)</i>'

    # step 0: add source title
    (extr_refs_named_unique
        .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__named__begin_title_end})
        .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__named__begin_in_title_continue})
        .count('refs_named_unique_src_0')
        .save('refs_named_unique_src_0')
    )

    actions.set_counters(*['cnt__matching_exact', 'cnt__sources_missing_updated'])
    cnt_tot = len(extr_refs_named_unique.get())

    # step 1: add source id using source title as reference
    (extr_refs_named_unique
        .addattr('source__id', actions.namedrefs__add_srcid, use_element=True)
        .count('refs_named_unique_src_1')
        .save('refs_named_unique_src_1')
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
    (extr_updated_sources
        .extend(
            Extractor(data=actions.get_legend('sources_missing'))
            .count('sources_missing_2')
            .save('sources_missing_2')
        )
        .count('sources_updated_2')
        .save('sources_updated_2')
    )

    actions.set_legends(**{
        'sources': extr_updated_sources.get()
    })
    
    print(f'missing sources: added {cntrs["cnt__sources_missing_updated"]} new sources.')
    print(f'sources: combined sources with missing sources, now available as a single list of {len(extr_updated_sources.get())} sources.')

    print('='*100)
# =============================
# 2.4 NAMED REFS again
# =============================
    print(f'\nNAMED REFS')

    actions.set_counters(*['cnt__sub_ref_found', 'cnt__not_a_source'])

    # step 2: begin adding sources as lists, starting from "multiple" refnames
    (extr_refs_named_unique
        .addattr('sources', [])
        .mapto(actions.mapto__namedrefs__add_srcid_multiple)
        .addattr('source__title', actions.namedrefs__add_missing_srctitle, use_element=True)
        .count('refs_named_unique_src_2')
        .save('refs_named_unique_src_2')
    )

    cntrs = actions.get_counters()
    print(
        f'[mapto__namedrefs__add_srcid_multiple]: out of {cnt_tot} named refs:\n'
        f'\t- {cnt_tot - cntrs["cnt__sub_ref_found"] - cntrs["cnt__not_a_source"]} refs were left unchanged.\n'
        f'\t- {cntrs["cnt__sub_ref_found"]} refs have a complex name (multiple tokens) but refer to a valid source, so they are secondary refs.\n'
        f'\t\tThose are marked with {TMP_MARKER_2} and will have is_secondary=True\n'
        f'\t- {cntrs["cnt__not_a_source"]} refs have a complex name (multiple tokens) which is invalid (not a source)\n'
        f'\t\tThose are marked with {TMP_MARKER_1} and will be removed.'
    )

    # step 3: finish converting source ids into lists and cleanup
    (extr_refs_named_unique
        .addattr('is_secondary', lambda **kwargs: kwargs['element'].source__id == TMP_MARKER_2, use_element=True)
        .filter_rows(lambda ref: ref.source__id != TMP_MARKER_1)
        .mapto(actions.mapto__namedrefs__convert_srcid_srctitle_to_sourcelist)
        .filter_cols(['rid', 'event__id', 'name', 'is_secondary', 'sources', 'desc'])
        .count('refs_named_unique_src_3')
        .save('refs_named_unique_src_3')
    )

    # TODO build source > ref dictionary