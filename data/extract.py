import glob
import json
import os
import re

from structs import Event, Ref

# refnames_movie = ['IM', 'TIH', 'IM2', 'T', 'CATFA', 'TA', 'IM3', 'TTDW', 'CATWS', 'GotG', 'AAoU', 'AM', 'CACW', 'DS', 'GotGV2', 'SMH', 'TR', 'BP', 'AIW', 'AMatW', 'CM', 'AE', 'SMFFH', ]
# refnames_shows = ['AoS', 'AC', 'AoSSS', 'I', 'DD', 'JJ', 'LC', 'IF', 'TD', 'TP', 'R', 'C&D', ]


class Extractor(object):
    """ 
    An Extractor maintains a list of elements, which can be subsequentially manipulated.
    """

    def __init__(self, infile=None, data=None):
        if not infile:
            self.data = data
        else:
            with open(os.path.join(os.path.dirname(__file__), f'{infile}.json')) as wrapper:
                self.data = json.loads(wrapper.read())

    def fork(self):
        """
        Returns a new copy of the Extractor, already initialized to the current state.
        """
        return Extractor(data=self.data)

    def get(self):
        """
        Returns current extractor data.
        """
        return self.data

    def filter_row(self, pred):
        """ 
        Filters data by selecting elements matching pred.
        """
        if callable(pred):
            self.data = list(filter(pred, self.data))
        else:
            raise NotImplementedError
        return self

    def filter_col(self, col_names: list):
        """
        Filters data by selecting the given attributes or keys for each element in data.
        Works with both objects and dicts, replaces elements with dicts containing the selected columns.
        """
        if not self.data:
            raise Exception("filter_col: Empty list")
        if isinstance(self.data[0], dict):
            self.data = [{col: d[col] for col in col_names} for d in self.data]
        elif isinstance(self.data[0], object):
            self.data = [{col: getattr(d, col) for col in col_names} for d in self.data]
        else:
            raise NotImplementedError
        return self

    def consume_key(self, key: str):
        """
        ** Must be called after filter_col **
        If data elements are dicts and key is in the dicts' keysets, "consumes" the key.
        Replaces elements with the key's corresponding values.
        """
        if not self.data:
            raise Exception("consume_key: Empty list")
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

    def addattr(self, attr, value):
        """
        Updates all elements, adding attribute `attr` with value `value`.
        If elements are dicts, adds (`attr`,`value`) kv-pair instead.
        """
        for elem in self.data:
            if isinstance(elem, dict):
                elem[attr] = value
            else:
                elem.attr = value
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

    def save(self, outpath=None):
        """
        Saves current data outpath, serialized as JSON. Default file name is extracted__{outpath}.json. 
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

        outpath = '' if not outpath else outpath
        with open(os.path.join(os.path.dirname(__file__), f'extracted__{outpath}.json'), 'w') as outfile:
            dict_data = list(map(__dictify, self.data))
            outfile.write(json.dumps(dict_data, indent=2, ensure_ascii=False))
        return self


if __name__ == "__main__":
    infile = 'parsed'

    for outfile in glob.glob(os.path.join(os.path.dirname(__file__), 'extracted__*')):
        pass
        os.remove(outfile)

    extr_refs = Extractor(infile)

    # extract all refs, flattened
    (extr_refs
        .mapto(lambda ev: Event.from_dict(**ev))  # Parse raw data into Event and Ref objects.
        # .filter_row(lambda ev: ev.file == "1900s")
        # .filter_row(lambda ev: any([ref.ref_name == "\u0001" for ref in ev.refs]))
        # .save('events')
        .filter_col(['refs'])
        .consume_key('refs')
        .flatten()
        # .save('refs')
     )

    # extract unique anonymous refs
    (extr_refs.fork()
        .filter_row(lambda ref: ref.ref_name is None)
        .unique()
        .sort()
        .filter_col(['eid', 'ref_desc', 'ref_links'])
        .count('anonymous refs')
        .save('refs_anon')
    )

    extr_refs_named = (extr_refs.fork()
        .filter_row(lambda ref: ref.ref_name is not None)
        .sort()
        .addattr('movie', 'provaprova') # TODO
    )

    # extract named refs
    (extr_refs_named.fork()
        .unique()
        .sort()
        .count('named refs')
        .save('refs_named')
    )

    # extract unique refnames
    refnames = (extr_refs_named.fork()
        .filter_col(['ref_name'])
        .consume_key('ref_name')
        .unique()
        .sort()
        .count('refnames')
        .save('refnames')
        .get()
    )

    extr_movies = Extractor('movies')
    extr_episodes = Extractor('episodes')

    episodes_refnames = (extr_episodes
        .fork()
        .filter_col(['series', 'refname'])
        .consume_key('refname')
        .get()
    )
