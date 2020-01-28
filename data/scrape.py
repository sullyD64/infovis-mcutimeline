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
    filename = f'{OUT_DIR}/char__{pagetitle}.json'
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
            # for i, line in enumerate(text, start=1):
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
                    print(f'{logprfx} Saving parsed template at {filename}.')
                    outfile.write(json.dumps(parsed_chartemp, indent=2, ensure_ascii=False))
                return True
            else:
                raise CharacterTemplateNotFoundError
        except (WikipageNotExistingError, CharacterTemplateNotFoundError) as e:
            print(f'{logprfx} [NO] {e}')
            return False


if __name__ == "__main__":
    for outfile in glob.glob(f'{OUT_DIR}/*'):
        os.remove(outfile)

    occurrences = {}

    testpages = ["Captain America", "Heimdall", "Infinity Stones", "notapage", "Groot", "Captain America", "Black Widow"]
    for page in testpages:
        if crawl_character(page):
            if page in occurrences.keys():
                occurrences[page] += 1
            else:
                occurrences[page] = 1

    print(f'\n{occurrences}')

    # infile_parsed = os.path.join(DIR, f'auto/extracted__29__events_stripped.json')
    # extr_events = (Extractor(infile_parsed)
    #     .select_cols(['links'])
    #     .consume_key('links')
    #     .flatten()
    #     .mapto(lambda text: wtp.parse(text).wikilinks[0].title)
    #     .save('test', 'characters')
    # )
