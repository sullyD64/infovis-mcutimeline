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


    # =============================
    # 1. SOURCES
    # =============================

    manual_dir = os.path.join(DIR, 'manual')
    extr_movies = Extractor(f'{manual_dir}/movies.json')
    extr_episodes = Extractor(f'{manual_dir}/episodes.json')

    def add_source_attrs(**kwargs):
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

    # extract sources by combining movies and tv episodes
    extr_sources = (extr_movies.fork()
        .addattr('title', add_source_attrs, use_element=True, **{'to_add': 't'})
        .addattr('details', add_source_attrs, use_element=True, **{'to_add': 'd'})
        .addattr('sid', add_source_attrs, use_element=True, **{'to_add': 's'})
        .addattr('type', 'film')
        .filter_cols(['sid', 'title', 'type', 'details'])
        .extend(extr_episodes
            .addattr('title', add_source_attrs, use_element=True, **{'to_add': 't'})
            .addattr('details', add_source_attrs, use_element=True, **{'to_add': 'd'})
            .addattr('sid', add_source_attrs, use_element=True, **{'to_add': 's'})
            .addattr('type', 'episode')
            .filter_cols(['sid', 'title', 'type', 'details'])
        )
        .count('sources')
        .save('sources')
    )
    sources = extr_sources.get()

    print('='*100)
    # =============================
    # 2. REFS
    # =============================

    infile_parsed = os.path.join(DIR, f'{OUT_DIR}/parsed.json')
    extr_refs = Extractor(infile_parsed)

    # extract all refs, flattened
    (extr_refs
        .mapto(lambda ev: Event.from_dict(**ev))  # Parse raw data into Event and Ref objects.
        .save('events')
        .consume_key('refs')
        .flatten()
        .filter_cols(['rid', 'event__id', 'ref_name', 'ref_desc'])
        .count('refs')
        .save('refs')
    )

    print('='*100)
    # =============================
    # 2.1 ANONYMOUS REFS
    # =============================

    # extract anonymous refs
    extr_refs_anon = (extr_refs.fork()
        .filter_rows(lambda ref: ref.ref_name is None) .unique()
        .sort()
        .count('anonymous refs')
        .save('refs_anon')
    )

    def refs__add_srctitle(**kwargs):
        ref = kwargs['element']
        if hasattr(ref, 'source__title') and ref.source__title:
            return ref.source__title
        output = None
        if ref.ref_desc:
            matches = re.match(re.compile(kwargs['pattern']), ref.ref_desc)
            if matches:
                text = matches.group(1)
                links = wtp.parse(text).wikilinks
                if links:
                    output = links[0].title
        return output

    # TODO refs__add_srctitle > consider if including $ or not (include Deleted Scenes, etc.)
    pattern__anon__begin_title_end = r'^<i>([^"]*)</i>$'
    pattern__anon__begin_in_title_continue = r'^In <i>([^"]*)</i>'

    # extracts valid anonymous refs, then adds source title
    (extr_refs_anon
        .filter_rows(lambda ref: re.match(pattern__anon__begin_title_end, ref.ref_desc))
        .addattr('source__title', refs__add_srctitle, use_element=True, **{'pattern': pattern__anon__begin_title_end})
        .extend(
            extr_refs_anon.fork()
            .filter_rows(lambda ref: re.match(pattern__anon__begin_in_title_continue, ref.ref_desc))
            .addattr('source__title', refs__add_srctitle, use_element=True, **{'pattern': pattern__anon__begin_in_title_continue})
        )
        .count('anonymous valid refs')
        .save('refs_anon_srctitle')
    )

    count_found, count_notfound = 0, 0
    count_updated_sources = 0
    count_tot = len(extr_refs_anon.get())

    def anonrefs__add_srcid(**kwargs):
        global count_found, count_notfound, count_updated_sources
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
        title = ref.source__title
        if clarification_found and not clarification_found[0] == "T.R.O.Y.":  # TODO anonrefs__add_srcid > workaround for Luke Cage 2.13 (find a better way)
            clarification = clarification_found[0]
            title = re.sub(r'(\([^\)]+\))$', '', ref.source__title).strip()
            print(f'[anonrefs__add_srcid] rid: {ref.rid} has clarification ({clarification}) in title: "{ref.source__title}"')

        for index, source in enumerate(sources):
            stitle = source['title']
            if (stitle.startswith(title)) and (not clarification or (clarification_matches(clarification, source))):
                found = True
                output = source['sid']
                if stitle.strip() != ref.source__title.strip():
                    sources[index]['title'] = ref.source__title
                    count_updated_sources += 1
                break
 
        if found:
            count_found += 1
        else:
            # TODO anonrefs__add_srcid > create missing source
            count_notfound += 1
            print(f'[anonrefs__add_srcid] rid: {ref.rid} refers to missing source: {title}')
    
        return output

    (extr_refs_anon
        .addattr('source__id', anonrefs__add_srcid, use_element=True)
        .save('refs_anon_srctitle_srcid')
    )
    print(f'[anonrefs__add_srcid]: sources added to [{count_found}/{count_tot}] anon refs (not found: {count_notfound})')

    print('='*100)
    # =============================
    # 2.2.1 Update sources
    # =============================

    print(f'updated sources: {count_updated_sources}')
    extr_sources = Extractor(data=sources)
    extr_sources.save('sources_updated')
    updated_sources = extr_sources.get()

    print('='*100)
    # =============================
    # 2.2 NAMED REFS
    # =============================

    # extract named refs
    extr_refs_named = (extr_refs.fork()
        .filter_rows(lambda ref: ref.ref_name is not None)
        .sort()
        .count('named refs')
        .save('refs_named')
    )
    extr_refs_named_unique = (extr_refs_named.fork()
        .unique()
        .sort()
        .count('unique named refs')
        .save('refs_named_unique')
    )

    count_found, count_notfound = 0, 0
    count_tot = len(extr_refs_named_unique.get())

    def namedrefs__add_srcid(**kwargs):
        global count_found, count_notfound
        ref = kwargs['element']
        refname, srctitle = ref.ref_name, ref.source__title
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
            count_found += 1
        else:
            count_notfound += 1
        return output

    def namedrefs__add_missing_srctitle(**kwargs):
        srcid = kwargs['element'].source__id
        output = None
        match = list(filter(lambda src: src['sid'] == srcid, sources))
        if match:
            output = match[0]['title']
        return output

    pattern__named__begin_title_end = r'^<i>([^"]*)</i>$'
    pattern__named__begin_in_title_continue = r'^In <i>([^"]*)</i>'

    (extr_refs_named_unique
        .addattr('source__title', refs__add_srctitle, use_element=True, **{'pattern': pattern__named__begin_title_end})
        .addattr('source__title', refs__add_srctitle, use_element=True, **{'pattern': pattern__named__begin_in_title_continue})
        .count('unique named refs with sourcetitle')
        .save('refs_named_unique_srctitle')
        .addattr('source__id', namedrefs__add_srcid, use_element=True)
        .addattr('source__title', namedrefs__add_missing_srctitle, use_element=True)
    )

    print(f'[namedrefs__add_srcid]: sources added to [{count_found}/{count_tot}] named refs (not found: {count_notfound})')

    (extr_refs_named_unique
        .filter_rows(lambda ref: ref.source__id and ref.source__title)
        .count('unique named refs with sourcetitle and sourceid')
        .save('refs_named_unique_srctitle_srcid')
    )

    # extract unique refnames
    refnames = (extr_refs_named.fork()
        .consume_key('ref_name')
        .unique()
        .sort()
        .count('unique refnames')
        .save('refnames_legend')
        .get()
    )
    
    # TODO build source > ref dictionary














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
    #                 # 'events': [ref.event__id],
    #             }
    #         else:
    #             reflegend[name]['count'] += 1
    #             # reflegend[name]['events'].append(ref.event__id)

    # with open(os.path.join(os.path.dirname(__file__), f'reflegend.json'), 'w') as outfile:
    #     outfile.write(json.dumps(reflegend, indent=2, ensure_ascii=False))
