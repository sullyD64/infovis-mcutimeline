# data_scripts/lib/logic.py
import copy
import itertools
import logging as log
import os
import pathlib
import re

import requests
import wikitextparser as wtp
from bs4 import BeautifulSoup

from data_scripts.lib import constants, errors, utils
from data_scripts.lib.structs import Struct


class Extractor(object):
    """
    An Extractor maintains a list of elements, which can be subsequentially manipulated by method chaining.
    Elements can be Objects or dictionaries.
    """
    PRFX = 'extr'
    SEP = '__'
    __code = None
    __dir = None
    __step = 0

    @classmethod
    def code(cls, code: str = None):
        cls.__step = 0
        cls.__code = code
        return cls

    @classmethod
    def cd(cls, outdir: pathlib.Path = None):
        cls.__dir = outdir
        return cls

    @classmethod
    def clean_output(cls):
        if not cls.__dir:
            raise errors.ExtractorOutdirMissingError
        for outfile in cls.__dir.glob(cls.SEP.join([cls.PRFX, cls.__code, '*'])):
            os.remove(outfile)
        log.warning(f'Cleaned: {cls.__code} files at {cls.__dir}')

    def __init__(self, infile: pathlib.Path = None, data: list = None):
        self.data = data if data else []
        if infile:
            with open(infile) as rawfile:
                self.data = utils.jloads(rawfile)
        self.prfx = Extractor.PRFX
        self.sep = Extractor.SEP

    def fork(self):
        """
        Returns a new Extractor containing a deep copy of its data.
        """
        return Extractor(data=copy.deepcopy(self.data))

    def extend(self, other):
        """
        Extends current data with other Extractor's data.
        """
        if isinstance(other, Extractor):
            self.data.extend(other.get())
        else:
            raise NotImplementedError
        return self

    def get(self):
        """
        Returns current data.
        """
        return self.data

    def get_first(self):
        """
        Returns the first element in data.
        """
        if not self.data:
            log.error("Empty list")
            return None
        return self.data[0]

    def parse_raw(self, clazz):
        """
        Parses raw data into given Struct object if clazz is a subclass of Struct.
        """
        if not self.data:
            log.error("Empty list")
            return self

        if issubclass(clazz, Struct):
            self.data = [clazz.from_dict(**d) for d in self.data]
            log.info(f'Parsing into objects of type {clazz}')
        else:
            raise errors.InvalidClassError(clazz)
        return self

    def filter_rows(self, pred):
        """
        Filters data by selecting elements matching pred.
        """
        if callable(pred):
            self.data = list(filter(pred, self.data))
        else:
            raise NotImplementedError
        return self

    def select_cols(self, col_names: list):
        """
        Filters data by selecting the given attributes or keys for each element in data.\n
        Works with both objects and dicts.
        """
        if not self.data:
            log.error("Empty list")
            return self

        if isinstance(self.data[0], dict):
            self.data = [{col: d[col] for col in col_names} for d in self.data]
        elif isinstance(self.data[0], Struct):
            clazz = self.data[0].__class__
            self.data = [d.to_dict() for d in self.data]
            self.data = [clazz.from_dict(**{col: d[col] for col in col_names}) for d in self.data]
        else:
            raise NotImplementedError
        return self

    def remove_cols(self, excluded_col_names: list):
        """
        Filters data by selecting all attributes or keys but the ones for each element in data.\n
        Works with both objects and dicts.
        """
        if not self.data:
            log.error("Empty list")
            return self

        if isinstance(self.data[0], dict):
            col_names = [key for key in self.data[0].keys() if key not in excluded_col_names]
        elif isinstance(self.data[0], Struct):
            col_names = [key for key in self.data[0].__dict__.keys() if key not in excluded_col_names]
        return self.select_cols(col_names)

    def consume_key(self, key: str):
        """
        WARNING: after calling consume_key, replaced elements are dicts containing the selected columns.\n
        If data elements are dicts and key is in the dicts' keysets, "consumes" the key.\n
        Replaces elements with the key's corresponding values.
        """
        if not self.data:
            log.error("Empty list")
            return self

        if not isinstance(self.data[0], dict):
            self.data = [d.to_dict(**{'ignore_nested': True}) for d in self.data]

        keys = self.data[0].keys()
        if key in keys:
            self.data = [d[key] for d in self.data]
        else:
            raise KeyError(key)
        return self

    def groupby(self, key_or_attr: str):
        """
        WARNING: data must be sorted first.
        If `key_or_attr` is a valid key or attribute name, replaces data with a list of dictionaries.\n
        Each dict is in the following format: {`key_or_attr`: grouping_key, 'elements': [grouped_elements]}.
        """
        if not self.data:
            log.error("Empty list")
            return self

        sample_el = self.data[0]

        def keyfunc(elem): return (
            elem[key_or_attr] if isinstance(sample_el, dict)
            else (getattr(elem, key_or_attr) if isinstance(sample_el, object)
                  else elem)
        )
        newdata = []
        for keyval, grouped_elements in itertools.groupby(self.data, keyfunc):
            newdata.append({
                key_or_attr: keyval,
                'elements': list(grouped_elements)
            })
        self.data = newdata
        return self

    def mapto(self, func, **kwargs):
        """
        Calls func for each element in data.\n
        Use kwargs to include optional variables and data structures.
        """
        self.data = [func(elem, **kwargs) for elem in self.data]
        return self

    def iterate(self, func, **kwargs):
        """
        WARNING: func must take data as first parameter and return a list of elements.\n
        Calls func to iterate data and extract a new list to replace data with.\n
        Use kwargs to include optional variables and data structures.\n
        This can be used instead of mapto to implement a complex forloop over the extractor's elements.
        """
        self.data = func(self.data, **kwargs)
        return self

    def addattr(self, attr, value_or_func, **kwargs):
        """
        Updates all elements, setting a new attribute `attr`.\n
        If elements are dicts, adds new key `attr` instead.\n
        If `value_or_func` is callable, it is called with kwargs as parameter.\n
        A reference to the current element is always passed as the first positional argument to the function.
        Use kwargs to include optional variables and data structures.\n
        """
        for elem in self.data:
            if callable(value_or_func):
                value = value_or_func(elem, **kwargs)
            else:
                value = value_or_func
            if isinstance(elem, dict):
                elem[attr] = value
            else:
                setattr(elem, attr, value)
        return self

    def flatten(self):
        """Flattens data (a list of lists) into a list."""
        self.data = [x for sublist in self.data for x in sublist]
        return self

    def sort(self, key_or_attr=None, reverse=False):
        """Sorts data."""
        if not self.data:
            log.error("Empty list")
            return self

        if key_or_attr:
            sample_el = self.data[0]

            def keyfunc(elem): return (
                elem[key_or_attr] if isinstance(sample_el, dict)
                else (getattr(elem, key_or_attr) if isinstance(sample_el, object)
                      else elem)
            )
            self.data = sorted(self.data, key=keyfunc, reverse=reverse)
        else:
            self.data = sorted(self.data, reverse=reverse)
        return self

    def unique(self):
        """If elements are hashable (aka they aren't dicts or lists), removes duplicate elements."""
        if not self.data:
            log.error("Empty list")
            return self

        if not isinstance(self.data[0], (dict, list)):
            self.data = list(set(self.data))
        else:
            raise NotImplementedError
        return self

    def count(self, what: str):
        """Prints the number of the extractor's elements"""
        log.info(f'{len(self.data):<5} {what}')
        return self

    def save(self, outfile: str = '', nostep=False):
        """
        Saves current data in outfile, serialized as JSON. File name is extracted__{code}__{step#}__{outfile}.json. \n
        Python objects anywhere in data are dictified. \n
        Save always calls count.
        """
        def __dictify(elem):
            """Readies data before dumping."""
            if isinstance(elem, Struct):
                return elem.to_dict()
            elif isinstance(elem, dict):
                for k, v in elem.items():
                    elem[k] = __dictify(v)
            elif isinstance(elem, list):
                elem = [__dictify(x) for x in elem]
            return elem

        Extractor.__step += 1
        if not Extractor.__dir:
            raise errors.ExtractorOutdirMissingError

        outfile_tkns = [Extractor.PRFX, Extractor.__code, f'{Extractor.__step:02d}', f'{outfile}.json']
        if nostep:
            outfile_tkns.pop(2)
            Extractor.__step -= 1
        outfile_fullname = Extractor.SEP.join(outfile_tkns)
        outpath = Extractor.__dir / outfile_fullname

        with open(outpath, 'w') as outfile:
            dict_data = [__dictify(elem) for elem in self.data]
            outfile.write(utils.jdumps(dict_data))

        self.count(outfile_fullname)
        log.debug(f'Saved {outpath.stem}')
        return self


class Parser(object):
    def parse_timeline(self, file: list, filename: str):
        from data_scripts.lib.structs import Event

        parsed_events = []
        htmlparser = utils.MyHTMLParser()

        curr_text = ''
        curr_day = None
        curr_month = None
        curr_year = None
        curr_reality = 'Main'   # current timeline or reality
        is_intro = True
        is_text_balanced = True

        for i, line in enumerate(file, start=1):
            line = str(line)

            # Skip first rows (intro, navigation + quote)
            if is_intro:
                if line.startswith('='):
                    is_intro = False
                else:
                    continue

            if line and not re.match(r'(^\[\[(File|Image|ru):|\{\{(Quote|Rewrite|Expand|(R|r)eflist)(\|)?)', line):
                if line.startswith('='):
                    heading = line.replace('=', '')
                    if re.match(r'^=====', line):
                        if heading == 'Real World':
                            heading = 'Main'
                        curr_reality = heading
                    elif re.match(r'^====', line):
                        curr_day = heading
                    elif re.match(r'^===', line):
                        curr_month = heading
                        curr_day = None
                    elif re.match(r'^==.', line):
                        if heading != 'References':
                            curr_year = heading
                            curr_month = None
                            curr_day = None
                else:
                    """
                    remove_files_or_images:             done before processing text, because sometimes images are displayed on a single line.
                    remove_empty_refs:                  remove empty <ref> tags which were left over by the previous removal of links/images.
                    fix_void_ref_nodes:                 done before processing text, because those tags are incorrectly parsed by htmlparser and thus cause errors in detecting balanced tags.
                    fix_incorrect_wikipedia_wiki_links: done before processing text, so that the wikitextparser doesn't recognise those links as wikilinks.
                    """
                    line = (utils.TextFormatter()
                            .text(line)
                            .remove_displayed_wiki_images_or_files_at_beginning()
                            .remove_empty_ref_nodes()
                            .fix_void_ref_nodes()
                            .fix_incorrect_wikipedia_wiki_links()
                            .get()
                            )

                    curr_text += line
                    if is_text_balanced:
                        if htmlparser.is_balanced(line):
                            line = re.sub(r'^\*', '', line)
                            ev = Event(filename, str(i), curr_text, curr_day, curr_month, curr_year, curr_reality)
                            curr_text = ''
                        else:
                            starting_i = i
                            is_text_balanced = False
                            continue
                    else:
                        if not htmlparser.is_balanced(line):
                            is_text_balanced = True
                            ev = Event(filename, f'{starting_i}-{i}', curr_text, curr_day, curr_month, curr_year, curr_reality)
                            curr_text = ''
                            starting_i = ''
                        else:
                            continue

                    parsed_events.append(ev)
        return parsed_events

    def parse_character(self, charid: str, chartemp: wtp.Template, templates: dict = None):
        tf = utils.TextFormatter()
        output = {}
        args_toselect = ['real name', 'alias', 'species', 'citizenship', 'gender', 'age', 'DOB', 'DOD', 'status', 'title', 'affiliation', 'actor', 'voice actor']
        selected = list(filter(lambda arg: arg.name.strip() in args_toselect, chartemp.arguments))

        output['cid'] = charid

        for arg in selected:
            clean_text = (tf.text(arg.value.strip()).remove_templates().strip_wiki_links().strip_small_html_tags().get()).split('<br>')
            if len(clean_text) == 1:
                clean_text = clean_text[0]
            output[arg.name.strip()] = clean_text

        not_selected = list(filter(lambda arg: arg.name.strip() not in [*args_toselect, *constants.MEDIA_TYPES_APPEARENCE], chartemp.arguments))
        for nsarg in not_selected:
            log.debug(f'Skipping arg "{nsarg.name.strip()}" : {nsarg.value.strip()}')

        appearences = []
        for media_arg in list(filter(lambda arg: arg.name.strip() in constants.MEDIA_TYPES_APPEARENCE, chartemp.arguments)):
            if media_arg:
                for app in media_arg.value.split('<br>'):
                    app_parsed = wtp.parse(app)
                    if app_parsed.wikilinks:
                        app = {
                            'source__title': app_parsed.wikilinks[0].title,
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
                    if el.split(' ')[0] in templates[tname].keys():
                        updated.append(f"{templates[tname][el.split(' ')[0]]} {''.join(el.split(' ')[1:])}".strip())
                    else:
                        updated.append(el)
                output[tname] = updated

        for tname in templates.keys():
            resolve_template(tname)

        return output


class Scraper(object):

    def __init__(self, code: str):
        self.code = code

    def crawl_text(self, pagetitle: str):
        html = requests.get(f"{constants.BASE_WIKI_URL}{pagetitle}?action=edit").text
        soup = BeautifulSoup(html, features="lxml")
        text = (soup.find_all(attrs={'id': 'wpTextbox1'})[0]).get_text().split('\n')
        if text[0]:
            return text
        else:
            raise errors.WikipageNotExistingError

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
                    parsed_temp[tkns[0]] = (utils.TextFormatter().text(tkns[1])
                                            .remove_displayed_wiki_images_or_files_at_beginning()
                                            .strip_wps_templates().strip_wiki_links()
                                            .get()
                                            ).strip()

                log.debug(f'{logprfx} [OK] Saved parsed template {pagetitle}.')
                extr_temp = Extractor(data=[parsed_temp]).save(f't__{pagetitle}', nostep=True)
                result = extr_temp.get_first()
            except errors.WikipageNotExistingError as e:
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
            except errors.WikipageNotExistingError as e:
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
