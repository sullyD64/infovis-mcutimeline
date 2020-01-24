import glob
import os
import re

from structs import Event
from logic import Extractor, ExtractorActions

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

    # extract sources by combining movies and tv episodes
    extr_sources = (extr_movies.fork()
        .addattr('title', actions.add_source_attrs, use_element=True, **{'to_add': 't'})
        .addattr('details', actions.add_source_attrs, use_element=True, **{'to_add': 'd'})
        .addattr('sid', actions.add_source_attrs, use_element=True, **{'to_add': 's'})
        .addattr('type', 'film')
        .filter_cols(['sid', 'title', 'type', 'details'])
        .extend(extr_episodes
            .addattr('title', actions.add_source_attrs, use_element=True, **{'to_add': 't'})
            .addattr('details', actions.add_source_attrs, use_element=True, **{'to_add': 'd'})
            .addattr('sid', actions.add_source_attrs, use_element=True, **{'to_add': 's'})
            .addattr('type', 'episode')
            .filter_cols(['sid', 'title', 'type', 'details'])
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
        .filter_rows(lambda ref: ref.name is None) .unique()
        .sort()
        .count('anonymous refs')
        .save('refs_anon')
    )

    # TODO refs__add_srctitle > consider if including $ or not (include Deleted Scenes, etc.)
    pattern__anon__begin_title_end = r'^<i>([^"]*)</i>$'
    pattern__anon__begin_in_title_continue = r'^In <i>([^"]*)</i>'

    # extracts valid anonymous refs, then adds source title
    (extr_refs_anon
        .filter_rows(lambda ref: re.match(pattern__anon__begin_title_end, ref.desc))
        .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__anon__begin_title_end})
        .extend(
            extr_refs_anon.fork()
            .filter_rows(lambda ref: re.match(pattern__anon__begin_in_title_continue, ref.desc))
            .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__anon__begin_in_title_continue})
        )
        .count('anonymous valid refs')
        .save('refs_anon_srctitle')
    )

    actions.set_legends(**{'sources': extr_sources.get()})
    actions.set_counters(*['count_found', 'count_notfound', 'count_updated_sources'])
    count_tot = len(extr_refs_anon.get())

    (extr_refs_anon
        .addattr('source__id', actions.anonrefs__add_srcid, use_element=True)
        .save('refs_anon_srctitle_srcid')
    )

    cntrs = actions.get_counters()
    print(f'[anonrefs__add_srcid]: sources added to [{cntrs["count_found"]}/{count_tot}] anon refs (not found: {cntrs["count_notfound"]})')

    print('='*100)
# =============================
# 2.2.1 Update sources
# =============================
    print(f'\nUPDATE SOURCES (after anonrefs__add_srcid, the title of some sources has been modified)')

    updated_sources = actions.get_legend('sources')
    Extractor(data=updated_sources).save('sources_updated')
    actions.set_legends(**{'sources': updated_sources})
    print(f'updated sources: {cntrs["count_updated_sources"]}')

    print('='*100)
# =============================
# 2.2 NAMED REFS
# =============================
    print(f'\nNAMED REFS')

    # extract named refs
    extr_refs_named = (extr_refs.fork()
        .filter_rows(lambda ref: ref.name is not None)
        .sort()
        .count('named refs')
        .save('refs_named')
    )
    extr_refs_named_unique = (extr_refs_named.fork()
        .unique()
        .sort()
        .count('unique named refs')
        .save('refs_named_unique')
    )

    actions.set_counters(*['count_found', 'count_notfound'])
    count_tot = len(extr_refs_named_unique.get())

    pattern__named__begin_title_end = r'^<i>([^"]*)</i>$'
    pattern__named__begin_in_title_continue = r'^In <i>([^"]*)</i>'

    (extr_refs_named_unique
        .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__named__begin_title_end})
        .addattr('source__title', actions.refs__add_srctitle, use_element=True, **{'pattern': pattern__named__begin_in_title_continue})
        .count('unique named refs with sourcetitle')
        .save('refs_named_unique_srctitle')
        .addattr('source__id', actions.namedrefs__add_srcid, use_element=True)
        .addattr('source__title', actions.namedrefs__add_missing_srctitle, use_element=True)
)
    cntrs = actions.get_counters()
    print(f'[namedrefs__add_srcid]: sources added to [{cntrs["count_found"]}/{count_tot}] named refs (not found: {cntrs["count_notfound"]})')

    (extr_refs_named_unique
        .count('unique named refs with sourcetitle and sourceid')
        .save('refs_named_unique_srctitle_srcid')
    )

    # extract unique refnames
    refnames = (extr_refs_named.fork()
        .consume_key('name')
        .unique()
        .sort()
        .count('unique refnames')
        .save('refnames_legend')
        .get()
    )

    # TODO build source > ref dictionary














    # def get_root(name: str):
    #     tkns = name.split(' ')
    #     prfx = tkns[0]
    #     sffx = ' '.join(tkns[1:])
    #     # if '/' in prfx:
    #     #     pass

    #     if re.match(r'^[A-Za-z&]+[0-9]{1,3}', prfx) and prfx in ep_refnames:
    #         series = (extr_episodes
    #             .filter_rows(lambda ep: ep['refname'] == prfx)
    #             .get())[0]['series']
    #         return series

    # named_refs = extr_refs_named.get()
    # for ref in named_refs:
    #     # name = ref.name
    #     name = get_root(ref.name)

    #     if name:
    #         if name not in reflegend.keys():
    #             reflegend[name] = {
    #                 'count': 1,
    #                 # 'events': [ref.event__id],
    #             }
    #         else:
    #             reflegend[name]['count'] += 1
    #             # reflegend[name]['events'].append(ref.event__id)

    # with open(os.path.join(os.path.dirname(__file__), f'reflegend.json'), 'w') as outfile:
    #     outfile.write(json.dumps(reflegend, indent=2, ensure_ascii=False))
