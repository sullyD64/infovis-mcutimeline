# mcu_app/processing.py
import json
import logging as log
from data_scripts.lib import constants, structs
from data_scripts.lib.logic import Extractor

def process_source_hierarchy(sources, hierarchy):
    extr_sources = (Extractor(data=sources)
        .parse_raw(structs.Source)
    )
    sources = extr_sources.get()
    sources_index = extr_sources.get_index('sid')
    sources_forest = json.loads(hierarchy)

    for tree in sources_forest:
        def recursive(node, level):
            node['val'] = sources[sources_index[node['val']]]
            node['level'] = level
            if not node['children']:
                return
            else:
                for child in node['children']:
                    recursive(child, level+1)
        for child in tree['children']:
            level = 0
            tree['level'] = level
            recursive(child, level+1)

    tree_film = next(iter([tree for tree in sources_forest if tree['val'] == constants.SRC_FILM_SERIES]))
    for filmseries in tree_film['children']:
        filmseries['children'] = sorted(filmseries['children'], 
            key=lambda film: int(film['val'].details['year']))
        filmseries['val'].details = {'beginning_year': next(iter(filmseries['children']))['val'].details['year']}
    tree_film['children'] = sorted(tree_film['children'], 
        key=lambda filmseries: filmseries['val'].details['beginning_year'])

    tree_tv = next(iter([tree for tree in sources_forest if tree['val'] == constants.SRC_TV_SERIES]))
    for tvseries in tree_tv['children']:
        tvseries['children'] = sorted(tvseries['children'], 
            key=lambda season: season['val'].details['season'])
        for tvseason in tvseries['children']:
            tvseason['children'] = sorted(tvseason['children'], 
                key=lambda episode: episode['val'].details['episode'])

    tree_comic = next(iter([tree for tree in sources_forest if tree['val'] == constants.SRC_COMIC]))
    tree_comic['children'] = sorted(tree_comic['children'], 
        key=lambda comic: comic['val'].title)

    return sources_forest