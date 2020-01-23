import glob
import json
import os
import re

from structs import Event, Ref
import wikitextparser as wtp

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
        print(f'{len(self.data)} {what}')
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


if __name__ == "__main__":
    for outfile in glob.glob(f'{OUT_DIR}/extracted__*'):
        os.remove(outfile)

    infile_parsed = os.path.join(DIR, f'{OUT_DIR}/parsed.json')
    extr_refs = Extractor(infile_parsed)

    # extract all refs, flattened
    (extr_refs
        .mapto(lambda ev: Event.from_dict(**ev))  # Parse raw data into Event and Ref objects.
        .save('events')
        .consume_key('refs')
        .flatten()
        .save('refs')
    )

    # =============================
    # ANONYMOUS REFS
    # =============================

    # extract anonymous refs
    extr_refs_anon = (extr_refs.fork()
        .filter_rows(lambda ref: ref.ref_name is None) .unique()
        .sort()
        .count('anonymous refs')
        .save('refs_anon')
    )

    def get_wikipage(**kwargs):
        mode, desc = kwargs['mode'], kwargs['element'].ref_desc
        if mode == 1:
            text = re.match(r'^<i>([^"]*)</i>', desc).group(1)
        if mode == 2:
            text = re.match(r'^In <i>([^"]*)</i>', desc).group(1)
        title = wtp.parse(text).wikilinks[0].title
        return title

    # extracts valid anonymous refs, then adds source title
    (extr_refs_anon
        .filter_rows(lambda ref: re.match(r'^<i>[^"]*</i>', ref.ref_desc))
        .addattr('source_title', get_wikipage, use_element=True, **{'mode': 1})
        .extend(
            extr_refs_anon.fork()
            .filter_rows(lambda ref: re.match(r'^In <i>[^"]*</i>', ref.ref_desc))
            .addattr('source_title', get_wikipage, use_element=True, **{'mode': 2})        )
        .count('anonymous valid refs')
        .save('refs_anon_valid')
    )

    manual_dir = os.path.join(DIR, 'manual')
    extr_movies = Extractor(f'{manual_dir}/movies.json')
    extr_episodes = Extractor(f'{manual_dir}/episodes.json')

    def add_source_attrs(**kwargs):
        to_add, source = kwargs['to_add'], kwargs['element']
        if to_add == 'd':
            out = {}
            for k, v in source.items():
                if k == 'refname':
                    continue
                out[k] = v
            return out
        elif to_add == 's':
            return source['refname']
    
    # extract sources by combining movies and tv episodes
    extr_episodes = (extr_episodes
        .addattr('details', add_source_attrs, use_element=True, **{'to_add': 'd'})
        .addattr('sid', add_source_attrs, use_element=True, **{'to_add': 's'})
        .addattr('type', 'episode')
        .filter_cols(['sid', 'type', 'details'])
    )
    extr_sources = (extr_movies.fork()
        .addattr('details', add_source_attrs, use_element=True, **{'to_add': 'd'})
        .addattr('sid', add_source_attrs, use_element=True, **{'to_add': 's'})
        .addattr('type', 'film')
        .filter_cols(['sid', 'type', 'details'])
        .extend(extr_episodes)
        .save('sources')
    )

    print('='*100)
    sources = extr_sources.get()
    count_found, count_notfound = 0, 0
    count_tot = len(extr_refs_anon.get())

    def add_source_anonrefs(**kwargs):
        global count_found, count_notfound, count_tot
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
        clarification_found = re.findall(r'\(([^\)]+)\)', ref.source_title)
        title = ref.source_title
        if clarification_found and not clarification_found[0] == "T.R.O.Y.":  # TODO add_source_anonrefs > workaround for Luke Cage 2.13 (find a better way)
            clarification = clarification_found[0]
            title = re.sub(r'(\([^\)]+\))$', '', ref.source_title).strip()
            print(f'[add_source_anonrefs] eid: {ref.eid} has clarification ({clarification}) in title: "{ref.source_title}"')

        for source in sources:
            if (source['details']['title'].startswith(title)) and (not clarification or (clarification_matches(clarification, source))):
                found = True
                output = source['sid']
 
        if found:
            count_found += 1
        else:
            # TODO add_source_anonrefs > create missing source
            count_notfound += 1
            print(f'[add_source_anonrefs] eid: {ref.eid} refers to missing source: {title}')
    
        return output

    (extr_refs_anon
        .addattr('source', add_source_anonrefs, use_element=True)
        # .filter_rows(lambda ref: not ref.source)
        .save('refs_anon_valid_sources')
    )
    print(f'{count_found}/{count_tot} (not found: {count_notfound})')

    print('='*100)

    # =============================
    # NAMED REFS
    # =============================

    # extract named refs
    extr_refs_named = (extr_refs.fork()
        .filter_rows(lambda ref: ref.ref_name is not None)
        .unique()
        .sort()
        .count('named refs')
        .save('refs_named')
    )

    # extract unique refnames
    refnames = (extr_refs_named.fork()
        .consume_key('ref_name')
        .unique()
        .sort()
        .count('unique refnames')
        .save('refnames')
        .get()
    )

    # TODO Extractor: extr_refs_named > add source attribute (check values against extr_sources, check if source or sub-source)
    # TODO Structs: Ref > add refid






















    # def get_root(name: str):
    #     tkns = name.split(' ')
    #     prfx = tkns[0]
    #     sffx = ' '.join(tkns[1:])
    #     # if '/' in prfx:
    #     #     pass

    #     if re.match(r'^[A-Za-z&]+[0-9]{1,3}', prfx) and prfx in ep_refnames:
    #         series = (extr_episodes
    #             .filter_rows(lambda ep: ep['refname'] == prfx)
    #             .get())[0]['series']
    #         return series

    # named_refs = extr_refs_named.get()
    # for ref in named_refs:
    #     # name = ref.ref_name
    #     name = get_root(ref.ref_name)

    #     if name:
    #         if name not in reflegend.keys():
    #             reflegend[name] = {
    #                 'count': 1,
    #                 # 'events': [ref.eid],
    #             }
    #         else:
    #             reflegend[name]['count'] += 1
    #             # reflegend[name]['events'].append(ref.eid)

    # with open(os.path.join(os.path.dirname(__file__), f'reflegend.json'), 'w') as outfile:
    #     outfile.write(json.dumps(reflegend, indent=2, ensure_ascii=False))
