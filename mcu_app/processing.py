# mcu_app/processing.py
import json
from data_scripts.lib import constants, structs
from data_scripts.lib.logic import Extractor

def process_source_hierarchy(sources, hierarchy):
    extr_sources = (Extractor(data=sources)
        .parse_raw(structs.Source)
    )
    sources = extr_sources.get()
    sources_index = extr_sources.get_index('sid')
    sources_hierarchy = json.loads(hierarchy)

    defaults = constants.HIERARCHY_INITIAL_ROOTS

    # 1. fetch Sources from sids stored in the trees' val fields. Use an index for performance.
    # >> add default=true to trees which will be initially selected
    for tree in sources_hierarchy:
        def recursive(node, default):
            node['val'] = sources[sources_index[node['val']]]
            node['default'] = default
            if not node['children']:
                return
            else:
                for child in node['children']:
                    recursive(child, default)
        
        tree['default'] = json.dumps(tree['val'] in defaults)
        for child in tree['children']:
            recursive(child, tree['default'])

    # 2. do all the required sorting
    # film tree: sort films by year, sort film series by year of the first movie for each series
    tree_film = next(iter([tree for tree in sources_hierarchy if tree['val'] == constants.SRC_FILM_SERIES]))
    for filmseries in tree_film['children']:
        filmseries['children'] = sorted(filmseries['children'], 
            key=lambda film: int(film['val'].details['year']))
        filmseries['val'].details = {'beginning_year': next(iter(filmseries['children']))['val'].details['year']}
    tree_film['children'] = sorted(tree_film['children'], 
        key=lambda filmseries: filmseries['val'].details['beginning_year'])

    # tv series tree: sort seasons by number, sort episodes by number
    tree_tv = next(iter([tree for tree in sources_hierarchy if tree['val'] == constants.SRC_TV_SERIES]))
    for tvseries in tree_tv['children']:
        tvseries['children'] = sorted(tvseries['children'], 
            key=lambda season: season['val'].details['season'])
        for tvseason in tvseries['children']:
            tvseason['children'] = sorted(tvseason['children'], 
                key=lambda episode: episode['val'].details['episode'])

    # comics tree: sort comics by name
    tree_comic = next(iter([tree for tree in sources_hierarchy if tree['val'] == constants.SRC_COMIC]))
    tree_comic['children'] = sorted(tree_comic['children'], 
        key=lambda comic: comic['val'].title)

    return sources_hierarchy
