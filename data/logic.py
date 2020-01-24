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

    def add_source_attrs(self, **kwargs):
        to_add, source = kwargs['to_add'], kwargs['element']
        if to_add == 'd':
            out = {}
            for k, v in source.items():
                if k in ['refname', 'title']:
                    continue
                out[k] = v
            return out
        elif to_add == 's':
            return source['refname']
        elif to_add == 't':
            return source['title']

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
                    self.counters['count_updated_sources'] += 1
                break
        if found:
            self.counters['count_found'] += 1
        else:
            # TODO anonrefs__add_srcid > create missing source
            self.counters['count_notfound'] += 1
            print(f'[anonrefs__add_srcid] rid: {ref.rid} refers to missing source: "{r__title}"')
        return output

    def namedrefs__add_srcid(self, **kwargs):
        sources = self.legends['sources']
        ref = kwargs['element']
        refname, srctitle = ref.name, ref.source__title
        found = False
        output = None
        matching_exact = list(filter(lambda src: src['sid'] == refname, sources))
        if matching_exact:
            output = matching_exact[0]['sid']
            found = True
        else:
            if len(refname.split(' ')) == 1:
                # TODO namedrefs__add_srcid > SINGLE TOKEN > determine if a source or not, then HANDLE_MISSING_SRCID or HANDLE_NOT_A_SOURCE
                pass
            else:
                # TODO namedrefs__add_srcid > MULTIPLE TOKENS > determine if tokens contain a matching srcid. If so, HANDLE_SUB_REF, else HANDLE_NOT_A_SOURCE
                pass
        if found:
            self.counters['count_found'] += 1
        else:
            self.counters['count_notfound'] += 1
        return output

    def namedrefs__add_missing_srctitle(self, **kwargs):
        sources = self.legends['sources']
        srcid = kwargs['element'].source__id
        output = None
        match = list(filter(lambda src: src['sid'] == srcid, sources))
        if match:
            output = match[0]['title']
        return output
