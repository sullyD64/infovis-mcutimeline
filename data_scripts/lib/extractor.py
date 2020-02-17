# data_scripts/lib/extractor.py

import copy
import itertools
import logging as log
import os
import pathlib

from data_scripts.lib.errors import ExtractorOutdirMissingError
from data_scripts.lib.utils import jdumps, jloads
from data_scripts.lib.structs import Event, Ref, Source


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
            raise ExtractorOutdirMissingError
        for outfile in cls.__dir.glob(cls.SEP.join([cls.PRFX, cls.__code, '*'])):
            os.remove(outfile)
        log.warning(f'Cleaned: {cls.__code} files at {cls.__dir}')

    def __init__(self, infile: pathlib.Path = None, data: list = None):
        self.data = data if data else []
        if infile:
            with open(infile) as rawfile:
                self.data = jloads(rawfile)
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
        if self.data:
            return self.data[0]
        raise NotImplementedError

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
            raise Exception("select_cols: Empty list")
        if isinstance(self.data[0], dict):
            self.data = [{col: d[col] for col in col_names} for d in self.data]
        elif isinstance(self.data[0], object):
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
        if isinstance(self.data[0], dict):
            col_names = [key for key in self.data[0].keys() if key not in excluded_col_names]
        elif isinstance(self.data[0], object):
            col_names = [key for key in self.data[0].__dict__.keys() if key not in excluded_col_names]
        return self.select_cols(col_names)

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

    def groupby(self, key_or_attr: str):
        """
        WARNING: data must be sorted first.
        If `key_or_attr` is a valid key or attribute name, replaces data with a list of dictionaries.\n
        Each dict is in the following format: {`key_or_attr`: grouping_key, 'elements': [grouped_elements]}.
        """
        sample_el = self.data[0]
        keyfunc = lambda elem: (
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

    def addattr(self, attr, value_or_func, use_element=False, **kwargs):
        """
        Updates all elements, setting a new attribute `attr`.\n
        If elements are dicts, adds new key `attr` instead.\n
        If `value_or_func` is callable, it is called with kwargs as parameter.\n
        If `use_element` is True, the element is passed as the first positional argument to the function.
        Use kwargs to include optional variables and data structures.\n
        """
        for elem in self.data:
            if callable(value_or_func):
                if use_element:
                    value = value_or_func(elem, **kwargs)
                else:
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

    def sort(self, key_or_attr=None, reverse=False):
        """Sorts data."""
        if key_or_attr:
            sample_el = self.data[0]  
            keyfunc = lambda elem: (
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
        if not isinstance(self.data[0], (dict, list)):
            self.data = list(set(self.data))
        else:
            raise NotImplementedError
        return self

    def count(self, what: str):
        """Prints the number of the extractor's elements"""
        log.info(f'[{len(self.data)}] {what}')
        return self

    def save(self, outfile: str = '', nostep=False):
        """
        Saves current data in outfile, serialized as JSON. File name is extracted__{code}__{step#}__{outfile}.json. \n
        Python objects anywhere in data are dictified. (see Event.to_dict() or Ref.to_dict()). \n
        Save always calls count.
        """
        def __dictify(elem):
            """Readies data before dumping."""
            if isinstance(elem, (Event, Ref, Source)):
                return elem.to_dict()
            elif isinstance(elem, dict):
                for k, v in elem.items():
                    elem[k] = __dictify(v)
            elif isinstance(elem, list):
                elem = [__dictify(x) for x in elem]
            return elem
        
        Extractor.__step += 1
        if not Extractor.__dir:
            raise ExtractorOutdirMissingError
        
        outfile_tkns = [Extractor.PRFX, Extractor.__code, f'{Extractor.__step:02d}', f'{outfile}.json']
        if nostep:
            outfile_tkns.pop(2)
            Extractor.__step -= 1
        outfile_fullname = Extractor.SEP.join(outfile_tkns)
        outpath = Extractor.__dir / outfile_fullname
        
        with open(outpath, 'w') as outfile:
            dict_data = [__dictify(elem) for elem in self.data]
            outfile.write(jdumps(dict_data))
        
        self.count(outfile_fullname)
        log.debug(f'Saved {outpath.stem}')
        return self
