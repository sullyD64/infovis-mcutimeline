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


def parse_character(charid: str, chartemp: wtp.Template):
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
    return output


def crawl_character(pagetitle: str):
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
                    parsed_chartemp = parse_character(pagetitle, chartemp)
                    print(f'{logprfx} [OK] Saving parsed template at {filename}.')
                    outfile.write(json.dumps(parsed_chartemp, indent=2, ensure_ascii=False))
                return True
            else:
                raise CharacterTemplateNotFoundError
        except (WikipageNotExistingError, CharacterTemplateNotFoundError) as e:
            print(f'{logprfx} [NO] {e}')
            return False


if __name__ == "__main__":
    inpath = os.path.join(DIR, f'auto/extracted__29__events_stripped.json')

    for outfile in glob.glob(f'{DIR}/auto/occurrences_*.json'):
        os.remove(outfile)

    # for outfile in glob.glob(f'{OUT_DIR}/*'):
    #     os.remove(outfile)
    # testpages = ["Captain America", "Heimdall", "Infinity Stones", "notapage", "Groot", "Captain America", "Black Widow"]
    
    all_links = (Extractor(inpath)
        .select_cols(['links'])
        .consume_key('links')
        .flatten()
        .mapto(lambda text: wtp.parse(text).wikilinks[0].title)
        .save('links')
        .get()
    )

    occ_chars = {}
    occ_nonchars = {}
    for i, page in enumerate(all_links, start=1):
        if page in occ_nonchars.keys():
            occ_nonchars[page] += 1  
        elif crawl_character(page):
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

    outpath_chars = os.path.join(DIR, 'auto/occurrences_chars.json')
    outpath_nonchars = os.path.join(DIR, 'auto/occurrences_nonchars.json')
    with open(outpath_chars, 'w') as outfile_chars, open(outpath_nonchars, 'w') as outfile_nonchars:
        outfile_chars.write(json.dumps(occ_chars, indent=2, ensure_ascii=False))
        outfile_nonchars.write(json.dumps(occ_nonchars, indent=2, ensure_ascii=False))
