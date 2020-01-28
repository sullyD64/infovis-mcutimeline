from bs4 import BeautifulSoup
import os
import json
import re
import requests
import wikitextparser as wtp

from utils import TextFormatter
from const import MEDIA_TYPES_APPEARENCE

BASE_URL = 'https://marvelcinematicuniverse.fandom.com/wiki/'
OUT_DIR = os.path.join(os.path.dirname(__file__), 'auto/characters')

class WikipageNotExistingError(Exception):
    def __init__(self, page):
        super().__init__(f"Wiki page not found: \"{page}\"")


class CharacterTemplateNotFoundError(Exception):
    def __init__(self, page):
        super().__init__(f"Character template not found: \"{page}\"")


def get_character_details(page: str):
    try:
        html = requests.get(f"{BASE_URL}{page}?action=edit").text
        soup = BeautifulSoup(html, features="lxml")
        text = (soup.find_all(attrs={'id': 'wpTextbox1'})[0]).get_text().split('\n')

        if not text[0]:
            raise WikipageNotExistingError(page)

        text_head = []
        for i, line in enumerate(text, start=1):
            text_head.append(line)
            if '{{' not in line and '}}' in line:
                break

        templates = wtp.parse(re.sub(r' +', ' ', (''.join(text_head)))).templates
        chartemp = list(filter(lambda t: "Character" in t.name, templates))
        chartemp = chartemp[0] if chartemp else None
        if chartemp:
            print(f'[Found] character template for {page}')
            appearences = {}

            tf = TextFormatter()
            for media in MEDIA_TYPES_APPEARENCE:
                m_apps = list(filter(lambda arg: media in arg.name, chartemp.arguments))
                m_apps = m_apps[0].value if m_apps else None

                if m_apps:
                    m_apps = m_apps.split('<br>')
                    for i, app in enumerate(m_apps):
                        parsed = wtp.parse(app)
                        if parsed.wikilinks:
                            source = tf.text(str(parsed.wikilinks[0])).strip_wiki_links().get()
                            tag = parsed.tags()
                            tag = tf.text(str(tag[0])).strip_small_html_tags().strip_wiki_links().get() if tag else None
                            m_apps[i] = {
                                'source': source,
                                'note': tag,
                            }
                    appearences[media] = m_apps

            outpath = (f'{OUT_DIR}/appearences__{page}.json')
            with open(outpath, 'w') as outfile:
                outfile.write(json.dumps(appearences, indent=2, ensure_ascii=False))
                print(f'[Done] output available at {outpath}')

            return appearences
        else:
            raise CharacterTemplateNotFoundError

    except WikipageNotExistingError as e:
        print(e)
    except CharacterTemplateNotFoundError as e:
        print(e)
        return None


if __name__ == "__main__":
    # TODO parametrize character details extraction
    page = 'Captain_America'
    appearences = get_character_details(page)
