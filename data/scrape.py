import glob
import json
import os
import re

import requests
import wikitextparser as wtp
from bs4 import BeautifulSoup

from const import BASE_URL, MEDIA_TYPES_APPEARENCE
from logic import Extractor
from utils import TextFormatter

DIR = os.path.dirname(__file__)
OUT_DIR = os.path.join(os.path.dirname(__file__), 'auto/characters')

class WikipageNotExistingError(Exception):
    def __init__(self):
        super().__init__(f'Wiki page not found.')


class CharacterTemplateNotFoundError(Exception):
    def __init__(self):
        super().__init__(f'Character template not found.')


def search_page(pagetitle: str):
    html = requests.get(f"{BASE_URL}{pagetitle}?action=edit").text
    soup = BeautifulSoup(html, features="lxml")
    text = (soup.find_all(attrs={'id': 'wpTextbox1'})[0]).get_text().split('\n')
    if text[0]:
        return text
    else:
        raise WikipageNotExistingError


def parse_character(charid: str, chartemp: wtp.Template, legend: dict = None):
    tf = TextFormatter()
    output = {}
    args_toselect = ['real name', 'alias', 'species', 'citizenship', 'gender', 'age', 'DOB', 'DOD', 'status', 'title', 'affiliation', 'actor', 'voice actor']
    selected = list(filter(lambda arg: arg.name.strip() in args_toselect, chartemp.arguments))

    output['cid'] = charid

    for arg in selected:
        clean_text = (tf.text(arg.value.strip()).remove_templates().strip_wiki_links().strip_small_html_tags().get()).split('<br>')
        if len(clean_text) == 1:
            clean_text = clean_text[0]
        output[arg.name.strip()] = clean_text

    not_selected = list(filter(lambda arg: arg.name.strip() not in [*args_toselect, *MEDIA_TYPES_APPEARENCE], chartemp.arguments))
    for nsarg in not_selected:
        print(f'\tskipping arg "{nsarg.name.strip()}" : {nsarg.value.strip()}')

    appearences = []
    for media_arg in list(filter(lambda arg: arg.name.strip() in MEDIA_TYPES_APPEARENCE, chartemp.arguments)):
        if media_arg:
            for app in media_arg.value.split('<br>'):
                app_parsed = wtp.parse(app)
                if app_parsed.wikilinks:
                    app = {
                        'source__title': tf.text(str(app_parsed.wikilinks[0])).strip_wiki_links().get(),
                        'source__type': media_arg.name.strip(),
                    }
                    notes = tf.text(str(app_parsed.tags()[0])).strip_small_html_tags().strip_wiki_links().get() if app_parsed.tags() else None
                    if notes:
                        app['notes'] = notes[1:-1]
                    appearences.append(app)
    output['appearences'] = appearences

    def resolve_template(tname: str):
        if tname in output.keys():
            updated = []
            if not isinstance(output[tname], list):
                output[tname] = [output[tname]]
            for el in output[tname]:
                if el.split(' ')[0] in legend[tname].keys():
                    updated.append(f"{legend[tname][el.split(' ')[0]]} {''.join(el.split(' ')[1:])}".strip())
                else:
                    updated.append(el)
            output[tname] = updated

    for tname in legend.keys():
        resolve_template(tname)

    return output


def crawl_character(pagetitle: str, legend: dict = None):
    pagetitle = re.sub(' ', '_', pagetitle)
    pagetitle_clean = re.sub(r"[\\\/]", "__", pagetitle)
    filename = f'{OUT_DIR}/char__{pagetitle_clean}.json'
    logprfx = f'[crawl_character] "{pagetitle}":'
    print(f'\n{logprfx[:-1]}')
    try:
        with open(filename):
            print(f'{logprfx} [OK] found in cache.')
            return True
    except FileNotFoundError:
        print(f'{logprfx} not found in cache, searching the wiki...')
        try:
            text = search_page(pagetitle)
            text_head = []
            for line in text:
                text_head.append(line)
                if '{{' not in line and '}}' in line:
                    break
            text_head_nowhitespace = re.sub(r' +', ' ', (''.join(text_head)))
            templates = wtp.parse(text_head_nowhitespace).templates
            chartemp = list(filter(lambda t: "Character" in t.name, templates))
            chartemp = chartemp[0] if chartemp else None
            if chartemp:
                with open(filename, 'w') as outfile:
                    print(f'{logprfx} Character template found. Parsing...')
                    parsed_chartemp = parse_character(pagetitle, chartemp, legend)
                    print(f'{logprfx} [OK] Saving parsed template at {filename}.')
                    outfile.write(json.dumps(parsed_chartemp, indent=2, ensure_ascii=False))
                return True
            else:
                raise CharacterTemplateNotFoundError
        except (WikipageNotExistingError, CharacterTemplateNotFoundError) as e:
            print(f'{logprfx} [NO] {e}')
            return False


def crawl_template(tname: str, filename: str):
    logprfx = f'[crawl_template] "{tname}":'
    try:
        with open(filename) as wrapper:
            print(f'{logprfx} [OK] found in cache.')
            return json.loads(wrapper.read())
    except FileNotFoundError:
        print(f'{logprfx} not found in cache, searching the wiki...')
        try:
            text = search_page(f"Template:{tname.capitalize()}")
            text_head = []
            for line in text[1:]:
                text_head.append(line)
                if '| default' in line:
                    break
            output = {}
            for line in text_head[:-1]:
                if not line.startswith('|'):
                    continue
                line = line[2:]
                tkns = line.split(' = ')
                output[tkns[0]] = TextFormatter().text(tkns[1]).remove_wiki_images_or_files().strip_wps_templates().strip_wiki_links().get().strip()
            with open(filename, 'w') as outfile:
                print(f'{logprfx} [OK] Saving parsed template at {filename}.')
                outfile.write(json.dumps(output, indent=2, ensure_ascii=False))
                return output
        except WikipageNotExistingError as e:
            raise Exception(f'FATAL {e}')


if __name__ == "__main__":
    inpath = os.path.join(DIR, f'auto/extracted__29__events_stripped.json')
    extr_events = Extractor(inpath)

    # for outfile in glob.glob(f'{DIR}/auto/templates/*.json'):
    #     os.remove(outfile)
    # for outfile in glob.glob(f'{OUT_DIR}/*'):
    #     os.remove(outfile)
    # for outfile in glob.glob(f'{DIR}/auto/occurrences_*.json'):
    #     os.remove(outfile)

    if not glob.glob(f'{DIR}/auto/templates/*.json'):
        affpath = f'{DIR}/auto/templates/template__affiliations.json'
        citpath = f'{DIR}/auto/templates/template__citizenship.json'
        legend = {}
        legend['affiliation'] = crawl_template('affiliation', affpath)
        legend['citizenship'] = crawl_template('citizenship', citpath)

    outpath_chars = os.path.join(DIR, 'auto/occurrences_chars.json')
    outpath_nonchars = os.path.join(DIR, 'auto/occurrences_nonchars.json')

    if not glob.glob(f'{DIR}/auto/occurrences_*.json'):
        # testpages = ["Captain America", "Heimdall", "Infinity Stones", "notapage", "Groot", "Captain America", "Black Widow"]
        # testpages = ["Thor"]

        all_links = (extr_events.fork()
            .select_cols(['links'])
            .consume_key('links')
            .flatten()
            .mapto(lambda text: wtp.parse(text).wikilinks[0].title)
            .save('links')
            .get()
        )

        # count occurrences for characters and non characters
        occ_chars = {}
        occ_nonchars = {}
        for i, page in enumerate(all_links, start=1):
            if page in occ_nonchars.keys():
                occ_nonchars[page] += 1
            elif crawl_character(page, legend):
                if page in occ_chars.keys():
                    occ_chars[page] += 1
                else:
                    occ_chars[page] = 1
            else:
                occ_nonchars[page] = 1

            max_chars = max(occ_chars, key=lambda k: occ_chars[k]) if occ_chars else None
            max_chars_count = occ_chars[max_chars] if max_chars else 0
            max_nonchars = max(occ_nonchars, key=lambda k: occ_nonchars[k]) if occ_nonchars else None
            max_nonchars_count = occ_nonchars[max_nonchars] if max_nonchars else 0
            print(
                f'\nPROGRESS: {i}/{len(all_links)} (remaining: {len(all_links)-i})\n'
                f'unique characters: {len(occ_chars)}\n'
                f'unique noncharacters: {len(occ_nonchars)}\n'
                f'most frequent character: {max_chars} ({max_chars_count})\n'
                f'most frequent noncharacter: {max_nonchars} ({max_nonchars_count})'
            )

        with open(outpath_chars, 'w') as outfile_chars, open(outpath_nonchars, 'w') as outfile_nonchars:
            outfile_chars.write(json.dumps(occ_chars, indent=2, ensure_ascii=False))
            outfile_nonchars.write(json.dumps(occ_nonchars, indent=2, ensure_ascii=False))
    else:
        occ_chars = json.loads(open(outpath_chars).read())
        occ_nonchars = json.loads(open(outpath_nonchars).read())

    
    def split_links(**kwargs):
        links = [wtp.parse(link).wikilinks[0].title for link in kwargs['element']['links']]
        output = list(filter(lambda link: link in kwargs['valid'].keys(), links))
        return output

    # split wikilinks in events between characters and non-characters
    (extr_events
        .addattr('characters', split_links, use_element=True, **{'valid': occ_chars})
        .addattr('characters_count', lambda **kwargs: len(kwargs['element']['characters']), use_element=True)
        .addattr('non_characters', split_links, use_element=True, **{'valid': occ_nonchars})
        .addattr('non_characters_count', lambda **kwargs: len(kwargs['element']['non_characters']), use_element=True)
        .remove_cols(['links'])
        .count('events_characters')
        .save('events_characters')
    )


    if glob.glob(f'{OUT_DIR}/*'):
        
        allchars = Extractor(data=[])
        for charfile in glob.glob(f'{OUT_DIR}/*'):
            allchars.extend(Extractor(data=[json.loads(open(charfile).read())]))


        unique_sources = {}
        def get_unique_sources(src: dict):
            if src['source__title'] not in unique_sources.keys():
                unique_sources[src['source__title']] = src['source__type']
            return src

        (allchars
            .count('allchars')
            .consume_key('appearences')
            .flatten()
            .select_cols(['source__type', 'source__title'])
            .mapto(get_unique_sources)
        )

        with open(os.path.join(DIR, 'auto/sources_types.json'), 'w') as outfile:
            outfile.write(json.dumps(unique_sources, indent=2, ensure_ascii=False))

