# data_scripts/s2_scrape.py
import logging as log

import wikitextparser as wtp

from data_scripts import logconfig
from data_scripts.lib import constants, errors
from data_scripts.lib.logic import Extractor, Scraper
from data_scripts.lib.pipeline import Actions

CODE = 's2'
CHARS_DIR = constants.PATH_INPUT_CRAWLED / 'characters'
TEMPS_DIR = constants.PATH_INPUT_CRAWLED / 'wikitemplates'
OUTPUT = constants.PATH_OUTPUT

### NUCLEAR SWITCHES
# clean_temps = True
# clean_chars = True
# clean_output = True
# clean_occ = True
quick = True

def main():
    log.getLogger().setLevel(log.INFO)
    log.info('### Begin ###')
    Extractor.code(CODE)
    actions = Actions()

    infile = next(OUTPUT.glob('*__events.json'), None)
    if not infile:
        raise errors.RequiredInputMissingError(CODE)
    extr_events = Extractor(infile).count('infile')
    scraper = Scraper(CODE)
    
    if 'clean_output' in globals():
        Extractor.cd(OUTPUT).clean_output()

    if 'clean_chars' in globals():
        Extractor.cd(CHARS_DIR).clean_output()

    if 'clean_temps' in globals():
        Extractor.cd(TEMPS_DIR).clean_output()

    # 1.1 crawl auxiliary templates for refining the character template
    Extractor.cd(TEMPS_DIR)
    templates = {}
    temp_aff_path = next(TEMPS_DIR.glob('*__affiliation.json'), None)
    templates['affiliation'] = Extractor(infile=temp_aff_path).get_first() if temp_aff_path else scraper.crawl_template(TEMPS_DIR, 'affiliation')
    temp_cit_path = next(TEMPS_DIR.glob('*__citizenship.json'), None)
    templates['citizenship'] = Extractor(infile=temp_cit_path).get_first() if temp_cit_path else scraper.crawl_template(TEMPS_DIR, 'citizenship')

    # 1.2 extract links list from events
    Extractor.cd(OUTPUT)
    links_path = next(OUTPUT.glob('*__events_links.json'), None)
    if links_path:
        extr_links = Extractor(infile=links_path)
    else:
        extr_links = (extr_events.fork()
            .select_cols(['links'])
            .consume_key('links')
            .flatten()
            .mapto(lambda text: wtp.parse(text).wikilinks[0].title)
            .save('events_links', nostep=True)
        )
    # extr_links = Extractor(data=["Captain America", "Heimdall", "Infinity Stones", "notapage", "Groot", "Captain America", "Jasper Sitwell", "Black Widow"]) # for testing only

    # 2. given all the parsed wikilinks, crawl characters and count occurrences of unique links 
    occ_chars_path = next(OUTPUT.glob('*__occ_chars.json'), None)
    occ_nonchars_path = next(OUTPUT.glob('*__occ_nonchars.json'), None)
    if 'clean_occ' in globals():
        occ_chars_path = None
        occ_nonchars_path = None
    actions.set_legends(**{
        'occ_chars': (
                (Extractor(infile=occ_chars_path)
                    .mapto(actions.s2__mapto__linkoccurrences__initialize)
                    .get_first()
                ) if occ_chars_path else {}
            ),
        'occ_nonchars': (
                (Extractor(infile=occ_nonchars_path)
                    .mapto(actions.s2__mapto__linkoccurrences__initialize)
                    .get_first()
                ) if occ_nonchars_path else {}
            ),
        'i': 1
    })
    Extractor.cd(CHARS_DIR)
    (extr_links
        .mapto(actions.s2__mapto__links__count_occurrences, **{
            'scraper': scraper,
            'templates': templates,
            'CHARS_DIR': CHARS_DIR,
            'tot': len(extr_links.get()),
            'quick': quick
        })
    )

    # 3. group characters when only the cid is different, and group their occurrence count too.
    Extractor.cd(OUTPUT)
    allchars_path = next(OUTPUT.glob('*__allchars.json'), None)
    if not quick or not allchars_path:
        extr_allchars = Extractor()
        for char_path in CHARS_DIR.glob('*__c__*'):
            extr_allchars.extend(Extractor(infile=char_path))
        (extr_allchars
            .count('allchars before grouping')
            .addattr('cid_redirects', [])
            .iterate(actions.s2__iterate__characters__group_alteregos)
            .count('allchars after grouping')
            .save('allchars', nostep=True)
        )
    else:
        extr_allchars = Extractor(infile=allchars_path)
    
    (Extractor(data=[actions.get_legend('occ_chars')])
        .mapto(actions.s2__mapto__linkoccurrences__finalize)
        .save('occ_chars', nostep=True)
        .mapto(actions.s2__mapto__linkoccurrences__rank)
        .save('occ_chars_ranked', nostep=True)
    )
    (Extractor(data=[actions.get_legend('occ_nonchars')])
        .mapto(actions.s2__mapto__linkoccurrences__finalize)
        .save('occ_nonchars', nostep=True)
        .mapto(actions.s2__mapto__linkoccurrences__rank)
        .save('occ_nonchars_ranked', nostep=True)
    )

    # 3. split wikilinks in parsed events between characters and non-characters using the two occurrence dicts
    occ_chars = Extractor(infile=next(OUTPUT.glob('*__occ_chars.json'))).get_first()
    occ_nonchars = Extractor(infile=next(OUTPUT.glob('*__occ_nonchars.json'))).get_first()
    (extr_events
        .addattr('characters', actions.s2__addattr__events__split_wikilinks, **{'valid': occ_chars})
        .addattr('non_characters', actions.s2__addattr__events__split_wikilinks, **{'valid': occ_nonchars})
        .remove_cols(['links'])
        .save('events_characters', nostep=True)
    )

    # 4. extract all the sources mentioned in the extracted characters
    actions.set_legends(**{'sourcetitles_chars': {}})
    (extr_allchars
        .consume_key('appearences')
        .flatten()
        .select_cols(['source__type', 'source__title'])
        .mapto(actions.s2__mapto__characters__get_unique_sources)
    )
    ordered_sourcetitles_chars = {k: v for (k,v) in sorted(actions.get_legend('sourcetitles_chars').items())}
    
    sourcetitles_chars_path = next(OUTPUT.glob('*__sourcetitles_chars.json'), None)
    if not sourcetitles_chars_path:
        (Extractor(data=[ordered_sourcetitles_chars])
            .save('sourcetitles_chars', nostep=True)
        )

    log.info('### End ###')

if __name__ == "__main__":
    logconfig.config()
    main()
