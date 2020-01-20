import glob
import json
import os
import re

from parse import Event, Ref

refnames_movie = ['IM', 'TIH', 'IM2', 'T', 'CATFA', 'TA', 'IM3', 'TTDW', 'CATWS', 'GotG', 'AAoU',
              'AM', 'CACW', 'DS', 'GotGv2', 'SMH', 'TR', 'BP', 'AIW', 'AMatW', 'CM', 'AE', 'SMFFH', ]

refnames_shows = ['AoS', 'AC', 'DD', 'JJ', 'LC', 'IF', 'TD', 'TP', 'R', 'C&D', ]


class Extractor(object):
    """ 
    An Extractor maintains a list of elements, which can be subsequentially manipulated.
    """

    def __init__(self, infile=None, data=None):
        if data:
            self.data = data
        else:
            with open(os.path.join(os.path.dirname(__file__), f'{infile}.json')) as wrapper:
                self.data = json.loads(wrapper.read())

    def fork(self):
        """
        Returns a new copy of the Extractor, already initialized to the current state.
        """
        return Extractor(data=self.data)

    def load_events(self):
        """
        Parses the raw data into Event and Ref objects.
        Subsequent operations are applied to those classes and make use of attributes instead of dict keys.
        """
        self.data = [Event.from_dict(**d) for d in self.data]
        return self

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
        if isinstance(self.data[0], object):
            self.data = [{col: getattr(d, col) for col in col_names} for d in self.data]
        elif isinstance(self.data[0], dict):
            self.data = [{col: d[col] for col in col_names} for d in self.data]
        else:
            raise NotImplementedError
        return self

    def consume_key(self, key: str):
        """
        ** Must be called after filter_col **
        If data elements are dicts and key is in the dicts' keysets, "consumes" the key.
        Replaces elements with the key's corresponding values.
        """
        keys = self.data[0].keys()
        if key in keys:
            self.data = [d[key] for d in self.data]
        else:
            raise KeyError(key)
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
        """Removes duplicates from data."""
        self.data = list(set(self.data))
        return self

    def output(self, outpath=None):
        def __dictify(elem):
            """Ready data for dumping."""
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
        # os.remove(outfile)

    refs = Extractor(infile)
    (refs
        .load_events()
        # .filter_row(lambda ev: ev.file == "1900s")
        .filter_col(['refs'])
        .consume_key('refs')
        .flatten()
        .output()
    )

    # extract anonymous refs
    refs_anon = (refs.fork()
        .filter_row(lambda ref: ref.ref_name is None)
        .filter_col(['eid', 'ref_desc', 'ref_links'])
        .output('refs_anon')
    )

    # extract named refs
    refs_named = (refs.fork()
        .filter_row(lambda ref: ref.ref_name is not None)
        .output('refs_named')
    )

    # extract unique refnames
    refnames = (refs_named.fork()
        .filter_col(['ref_name'])
        .consume_key('ref_name')
        .unique()
        .sort()
        .output('refnames')
    )
