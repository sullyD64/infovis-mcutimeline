import json
import os
import re

import wikitextparser as wtp

from structs import Event, Ref
from utils import TMP_MARKER_1, TMP_MARKER_2

DIR = os.path.dirname(__file__)
OUT_DIR = os.path.join(DIR, 'auto')


class Extractor(object):
    """
    An Extractor maintains a list of elements, which can be subsequentially manipulated.
    """

    def __init__(self, infile: str = None, data: list = None):
        if not infile:
            self.data = data
        else:
            with open(infile) as wrapper:
                self.data = json.loads(wrapper.read())

    def fork(self):
        """
        Returns a new copy of the Extractor, already initialized to the current state.
        """
        return Extractor(data=self.data)

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
        Returns current extractor data.
        """
        return self.data

    def filter_rows(self, pred):
        """
        Filters data by selecting elements matching pred.
        """
        if callable(pred):
            self.data = list(filter(pred, self.data))
        else:
            raise NotImplementedError
        return self

    def filter_cols(self, col_names: list):
        """
        Filters data by selecting the given attributes or keys for each element in data.\n
        Works with both objects and dicts.
        """
        if not self.data:
            raise Exception("filter_cols: Empty list")
        if isinstance(self.data[0], dict):
            self.data = [{col: d[col] for col in col_names} for d in self.data]
        elif isinstance(self.data[0], object):
            clazz = self.data[0].__class__
            self.data = [d.to_dict() for d in self.data]
            self.data = [clazz.from_dict(**{col: d[col] for col in col_names}) for d in self.data]
        else:
            raise NotImplementedError
        return self

    def consume_key(self, key: str):
        """
        WARNING: after calling consume_key, replaced elements are dicts containing the selected columns.\n
        If data elements are dicts and key is in the dicts' keysets, "consumes" the key.\n
        Replaces elements with the key's corresponding values.
        """
        if not self.data:
            raise Exception("consume_key: Empty list")

        if not isinstance(self.data[0], dict):
            self.data = [d.to_dict(**{'ignore_nested': True}) for d in self.data]

        keys = self.data[0].keys()
        if key in keys:
            self.data = [d[key] for d in self.data]
        else:
            raise KeyError(key)
        return self

    def mapto(self, func):
        """Applies func to all elements in data."""
        self.data = list(map(func, self.data))
        return self

    def addattr(self, attr, value_or_func, use_element=False, **kwargs):
        """
        Updates all elements, setting a new attribute `attr`.\n
        If elements are dicts, adds new key `attr` instead.\n
        If `value_or_func` is callable, it is called with kwargs as parameter.\n
        If `use_element` is True, the element is passed to the function in kwargs, under key 'element'.
        """
        for elem in self.data:
            if callable(value_or_func):
                if use_element:
                    kwargs['element'] = elem
                value = value_or_func(**kwargs)
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

    def sort(self):
        """Sorts data."""
        self.data = sorted(self.data)
        return self

    def unique(self):
        """If elements are hashable (aka they aren't dicts), removes duplicate elements."""
        if not isinstance(self.data[0], dict):
            self.data = list(set(self.data))
        else:
            raise NotImplementedError
        return self

    def count(self, what: str):
        """Prints the number of current elements"""
        print(f'COUNT: {what} ({len(self.data)})')
        return self

    def save(self, outfile=None):
        """
        Saves current data in outfile, serialized as JSON. Default file name is extracted__{outfile}.json. \n
        Python objects anywhere in data are dictified. (see Event.to_dict() or Ref.to_dict())
        """
        def __dictify(elem):
            """Readies data before dumping."""
            if isinstance(elem, (Event, Ref)):
                return elem.to_dict()
            elif isinstance(elem, dict):
                for k, v in elem.items():
                    elem[k] = __dictify(v)
            elif isinstance(elem, list):
                elem = [__dictify(x) for x in elem]
            return elem

        outfile = '' if not outfile else outfile
        with open(f'{OUT_DIR}/extracted__{outfile}.json', 'w') as outfile:
            dict_data = list(map(__dictify, self.data))
            outfile.write(json.dumps(dict_data, indent=2, ensure_ascii=False))
        return self


class ExtractorActions():

    def __init__(self):
        self.counters = {}
        self.legends = {}

    def set_legends(self, **kwargs):
        lgnds = {}
        for k, v in kwargs.items():
            lgnds[k] = v
        self.legends = lgnds

    def get_legend(self, lname):
        return self.legends[lname]

    def set_counters(self, *args):
        cntrs = {}
        for v in args:
            cntrs[v] = 0
        self.counters = cntrs

    def get_counters(self):
        return self.counters

    # ----------------------------

    def source__add_attrs(self, **kwargs):
        to_add, source = kwargs['to_add'], kwargs['element']
        if to_add == 'd':
            out = {}
            for k, v in source.items():
                if k in ['sid', 'title']:
                    continue
                out[k] = v
            return out
        elif to_add == 's':
            return source['sid']
        elif to_add == 't':
            return source['title']

    def mapto__source__extend_series_season(self, src):
        series_seasons = self.legends['series_seasons']

        def get_season_title(title, number):
            switcher = {
                1: 'One',
                2: 'Two',
                3: 'Three',
                4: 'Four',
                5: 'Five',
                6: 'Six',
            }
            return f'{title}/Season {switcher[number]}'

        if src['type'] == 'episode':
            src_season_details = {k: v for k,v in src['details'].items()}
            src_season_details.pop('episode')
            src_season = {
                'sid': src_season_details.pop('season_id'),
                'title': get_season_title(src_season_details['series'], src_season_details['season']),
                'type': 'season',
                'details': src_season_details
            }
            src_series_details = {k: v for k, v in src_season_details.items()}
            src_series_details.pop('season')
            src_series = {
                'sid': src_series_details['series_id'],
                'title': src_series_details['series'],
                'type': 'series',
                'details': {},
            }
            if src_season not in series_seasons:
                series_seasons.append(src_season)
            if src_series not in series_seasons:
                series_seasons.append(src_series)
        return src

    def refs__add_srctitle(self, **kwargs):
        ref = kwargs['element']
        if hasattr(ref, 'source__title') and ref.source__title:
            return ref.source__title
        output = None
        if ref.desc:
            matches = re.match(re.compile(kwargs['pattern']), ref.desc)
            if matches:
                text = matches.group(1)
                links = wtp.parse(text).wikilinks
                if links:
                    output = links[0].title
        return output

    def anonrefs__add_srcid(self, **kwargs):
        sources = self.legends['sources']
        sources_missing = self.legends['sources_missing']
        ref = kwargs['element']
        found = False
        output = None

        def clarification_matches(clarif: str, source: dict):
            type_matches = clarif in source['type']
            series_matches = False
            if 'series' in source['details'].keys():
                series_matches = clarif in source['details']['series']
            return any([type_matches, series_matches])

        clarification = None
        clarification_found = re.findall(r'\(([^\)]+)\)', ref.source__title)
        r__title = ref.source__title
        if clarification_found and not clarification_found[0] == "T.R.O.Y.":  # TODO anonrefs__add_srcid > workaround for Luke Cage 2.13 (find a better way)
            clarification = clarification_found[0]
            r__title = re.sub(r'(\([^\)]+\))$', '', ref.source__title).strip()
            print(f'[anonrefs__add_srcid] rid: {ref.rid} has clarification ({clarification}) in title: "{ref.source__title}"')

        for index, source in enumerate(sources):
            s__title = source['title']
            if (s__title.startswith(r__title)) and (not clarification or (clarification_matches(clarification, source))):
                found = True
                output = source['sid']
                if s__title.strip() != ref.source__title.strip():
                    print(f'[anonrefs__add_srcid] sources: adding clarification to title "{s__title}" => "{ref.source__title}"')
                    sources[index]['title'] = ref.source__title
                    self.counters['cnt_updated_sources'] += 1
                break
        if found:
            self.counters['cnt_found'] += 1
        else:
            sources_missing.append({
                'sid': None,
                'title': r__title,
                'type': 'other',
                'details': {}
            })
            self.counters['cnt_notfound'] += 1
            print(f'[anonrefs__add_srcid] rid: {ref.rid} refers to missing source: "{r__title}". Adding new source.')
        return output

    def namedrefs__add_srcid(self, **kwargs):
        ref = kwargs['element']
        output = None
        matching_exact = list(filter(lambda src: src['sid'] == ref.name, self.legends['sources']))
        if matching_exact:
            output = matching_exact[0]['sid']
            self.counters['cnt__matching_exact'] += 1
        else:
            tkns = ref.name.split(' ')
            if len(tkns) == 1:
                sources_missing = self.legends['sources_missing']
                output = ref.name
                in_sources_missing = []
                if ref.source__title:
                    in_sources_missing = list(filter(lambda src: src['title'] == ref.source__title, sources_missing))
                if ref.source__title and in_sources_missing:
                    in_sources_missing[0]['sid'] = ref.name
                else:
                    new_src = {
                        'sid': ref.name,
                        'title': ref.source__title,
                        'type': 'other',
                        'details': {}
                    }
                    sources_missing.append(new_src)

                    # special cases
                    # TODO namedrefs__add_srcid > workaround for special cases (WHiH, SE2010, TAPBWS, PT) (find a better way)
                    if tkns[0].startswith('WHiH'): 
                        new_src['type'] = 'web_series'
                        new_src['details']['series'] = 'WHiH Newsfront (web series)'
                        new_src['details']['series_id'] = 'WHiH'
                        new_src_main = {
                            'sid': 'WHiH',
                            'title': 'WHiH Newsfront (web series)',
                            'type': 'web_series',
                            'details': {}
                        }
                        if new_src_main not in sources_missing:
                            sources_missing.append(new_src_main)
                    elif tkns[0].startswith('SE2010'):
                        new_src['details']['series'] = 'Stark Expo/Promotional Campaign'
                        new_src['details']['series_id'] = 'SE2010'
                        new_src_main = {
                            'sid': 'SE2010',
                            'title': 'Stark Expo/Promotional Campaign',
                            'type': 'other',
                            'details': {}
                        }
                        if new_src_main not in sources_missing:
                            sources_missing.append(new_src_main)
                    elif tkns[0].startswith('TAPBWS'):
                        new_src['details']['series'] = 'The Avengers Prelude: Black Widow Strikes'
                        new_src['details']['series_id'] = 'TAPBWS'
                        new_src_main = {
                            'sid': 'TAPBWS',
                            'title': 'The Avengers Prelude: Black Widow Strikes',
                            'type': 'other',
                            'details': {}
                        }
                        if new_src_main not in sources_missing:
                            sources_missing.append(new_src_main)
                    elif tkns[0].startswith('PT'):
                        new_src['details']['series'] = 'Pym Technologies'
                        new_src['details']['series_id'] = 'PT'
                        new_src_main = {
                            'sid': 'PT',
                            'title': 'Pym Technologies',
                            'type': 'other',
                            'details': {}
                        }
                        if new_src_main not in sources_missing:
                            sources_missing.append(new_src_main)
                self.counters['cnt__sources_missing_updated'] += 1
            else:
                # simply mark the refs with a complex refname to iterate again after.
                output = TMP_MARKER_1
        return output
    
    def mapto__namedrefs__add_srcid_multiple(self, ref):
        allsources = self.legends['sources'] 
        if ref.source__id == TMP_MARKER_1:
            allsids = list(filter(lambda x: x is not None, [src['sid'] for src in allsources]))
            found = False
            prfx = ref.name.split(' ')[0]
            if prfx in allsids:
                found = True
                ref.sources = [prfx]
            elif any([sid.startswith(prfx) for sid in allsids]):
                raise Exception(f"Found prfx in any startswith {ref.name}, should find none")
            else:
                if any(char in prfx for char in ['/', '-']):
                    sources = []
                    sub_prfxs_1 = prfx.split('/')
                    found_multiple = False
                    for sp in sub_prfxs_1:
                        if '-' in sp:
                            sub_prfxs_2 = sp.split('-')
                            start_ep = sub_prfxs_2[0]
                            if start_ep in allsids:
                                start_ep_src = list(filter(lambda src: src['sid'] == start_ep, allsources))[0]
                                episodes = list(filter(lambda src: 'season_id' in src['details'] and src['details']['season_id'] == start_ep_src['details']['season_id'], allsources))
                                start = int(start_ep_src['details']['episode'])
                                end = int(sub_prfxs_2[1])
                                found_multiple = True
                                sources = [*sources, *[ep['sid'] for ep in episodes[start-1:end]]]
                        else:
                            if sp in allsids:
                                found_multiple = True
                                sources.append(sp)
                    if found_multiple:
                        print(f"[mapto__namedrefs__add_srcid_multiple] multiple sources found based on the prefix: {ref.name}")
                        found = True
                        ref.sources = [*ref.sources, *sources]
            if found:
                self.counters['cnt__sub_ref_found'] += 1
                ref.source__id = TMP_MARKER_2
            else:
                print(f'[mapto__namedrefs__add_srcid_multiple] not a source: {ref.name}')
                self.counters['cnt__not_a_source'] += 1      
        return ref

    def namedrefs__add_missing_srctitle(self, **kwargs):
        allsources = self.legends['sources']
        ref = kwargs['element']
        output = None
        if ref.source__title:
            output = ref.source__title
        else:
            match = list(filter(lambda src: src['sid'] == ref.source__id, allsources))
            if match:
                output = match[0]['title']
        return output

    def mapto__refs__convert_srcid_srctitle_to_sourcelist(self, ref):
        if len(ref.sources) == 0:
            ref.sources = [ref.source__id]
        return ref
