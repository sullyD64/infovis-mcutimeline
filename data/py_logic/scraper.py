# py_logic/scraper.py
import logging as log
import pathlib
import re

import requests
import wikitextparser as wtp
from bs4 import BeautifulSoup

from py_logic.extractor import Extractor
from py_logic.formatter import TextFormatter
from py_logic.parser import Parser
from py_utils.constants import BASE_WIKI_URL
from py_utils.errors import WikipageNotExistingError


class Scraper(object):

    def __init__(self, code: str):
        self.code = code

    def crawl_text(self, pagetitle: str):
        html = requests.get(f"{BASE_WIKI_URL}{pagetitle}?action=edit").text
        soup = BeautifulSoup(html, features="lxml")
        text = (soup.find_all(attrs={'id': 'wpTextbox1'})[0]).get_text().split('\n')
        if text[0]:
            return text
        else:
            raise WikipageNotExistingError

    def crawl_template(self, savepath: pathlib.Path, pagetitle: str):
        logprfx = f'"{pagetitle}":'
        result = None
        try:
            infile = next(savepath.glob(f'*__{pagetitle}.json'))
            extr_temp = Extractor(infile=infile)
            result = extr_temp.get_first()
            log.debug(f'{logprfx} [OK] found in cache.')
        except (StopIteration, FileNotFoundError):
            log.debug(f'{logprfx} not found in cache, searching the wiki...')
            try:
                pagetitle_prfx = f"Template:{pagetitle.capitalize()}"
                text = self.crawl_text(pagetitle_prfx)
                text_head = []
                for line in text[1:]:
                    text_head.append(line)
                    if '| default' in line:
                        break
                parsed_temp = {}
                for line in text_head[:-1]:
                    if not line.startswith('|'):
                        continue
                    line = line[2:]
                    tkns = line.split(' = ')
                    parsed_temp[tkns[0]] = (TextFormatter().text(tkns[1])
                        .remove_displayed_wiki_images_or_files_at_beginning()
                        .strip_wps_templates().strip_wiki_links()
                        .get()
                    ).strip()

                log.debug(f'{logprfx} [OK] Saved parsed template {pagetitle}.')
                extr_temp = Extractor(data=[parsed_temp]).save(f't__{pagetitle}', nostep=True)
                result = extr_temp.get_first()
            except WikipageNotExistingError as e:
                raise Exception(f'FATAL {e}')
        return result

    def crawl_character(self, savepath: pathlib.Path, charid: str, legend_templates: dict = None):
        logprfx = f'"{charid}":'
        log.debug(f'{logprfx[:-1]}')
        result = None
        try:
            pagetitle = re.sub(' ', '_', charid)
            pagetitle_clean = self.get_clean_pagetitle(pagetitle)
            extr_char = Extractor(next(savepath.glob(f'*__{pagetitle_clean}.json')))
            result = extr_char.get()
            log.debug(f'{logprfx} [OK] found in cache.')
        except (StopIteration, FileNotFoundError):
            log.debug(f'{logprfx} not found in cache, searching the wiki...')            
            try:
                text = self.crawl_text(pagetitle)
                text_head = []
                for line in text:
                    text_head.append(line)
                    if '{{' not in line and '}}' in line:
                        break
                text_head_nowhitespace = re.sub(r' +', ' ', (''.join(text_head)))
                chartemp = list(filter(lambda t: "Character" in t.name, wtp.parse(text_head_nowhitespace).templates))
                chartemp = chartemp[0] if chartemp else None
                if chartemp:
                    log.debug(f'{logprfx} Character template found. Parsing...')
                    parsed_chartemp = Parser().parse_character(charid, chartemp, legend_templates)
                    extr_char = Extractor(data=[parsed_chartemp]).save(f'c__{pagetitle_clean}', nostep=True)
                    result = extr_char.get()
                    log.debug(f'{logprfx} [OK] Saved parsed template for {pagetitle_clean}.')
                else:
                    log.debug(f'{logprfx} [NO] Character template not found.')
            except WikipageNotExistingError as e:
                log.debug(f'{logprfx} [NO] {e}')
        return result

    def is_cached(self, savepath: pathlib.Path, charid: str):
        if charid == "Ego":
            print(True)
        logprfx = f'"{charid}":'
        result = False
        clean_pagetitle = self.get_clean_pagetitle(charid)
        if next(savepath.glob(f'*__c__{clean_pagetitle}.json'), None):
            log.debug(f'{logprfx} [OK] found in cache.')
            result = True
        return result

    def get_clean_pagetitle(self, pagetitle: str):
        return re.sub(r"[\\\/]", "__", re.sub(' ', '_', pagetitle))
