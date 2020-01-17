import json
import os
import re
from html.parser import HTMLParser

from parse import Ref

refs_movie = ['IM', 'TIH', 'IM2', 'T', 'CATFA', 'TA', 'IM3', 'TTDW', 'CATWS', 'GotG', 'AAoU',
              'AM', 'CACW', 'DS', 'GotGv2', 'SMH', 'TR', 'BP', 'AIW', 'AMatW', 'CM', 'AE', 'SMFFH', ]

refs_shows = ['AoS', 'AC', 'DD', 'JJ', 'LC', 'IF', 'TD', 'TP', 'R', 'C&D', ]



# def enrich_refs(refs):




if __name__ == "__main__":
    inpath = os.path.join(os.path.dirname(__file__), 'parsed.json')
    outpath = os.path.join(os.path.dirname(__file__), 'extracted.json')

    with open(inpath) as wrapper:
        data = json.loads(wrapper.read())
        
        # filter event columns
        selected_columns = [
            {
                'id': d['id'],
                # 'file': d['file'],
                # 'line': d['line'],
                # 'date': d['date'],
                # 'reality': d['reality'],
                # 'title': d['title'],
                # 'desc': d['desc'],
                # 'multiple': d['multiple']
                # 'links': d['links'],
                'refs': d['refs'],
            } for d in data
        ]   
        events = selected_columns
        
        # filter event rows
        # events = list(filter(lambda ev: ev['reality'] == 'Sakaar', events))
        # events = list(filter(lambda ev: ev['multiple'], events))
        # events = list(filter(lambda ev: ev['id'] == 9842, selected_columns))

        
        events_refs = [ev['refs'] for ev in data]  # refs extraction
        refset = [ref for sublist in events_refs for ref in sublist]  # flatten to a single list of refs

        selected_columns_refs = [
            Ref.from_dict(**d) for d in refset
        ]
        refs = selected_columns_refs

        # extract non-named refs:   
        anon_refs = list(filter(lambda x: x.ref_name is None, refs))

        # extract named refs:
        named_refs = list(filter(lambda x: x.ref_name is not None, refs))

        # extract unique refnames
        ref_names = [x.ref_name for x in named_refs]
        
        # out_data = anon_refs
        out_data = named_refs
        # out_data = sorted(list(set(ref_names)))
        # out_data = refs

    with open(outpath, 'w') as outfile:

        out_dict_data = [ref.__dict__ for ref in out_data]
        # out_dict_data = [
        #     {
        #         # 'eid': d['eid'],
        #         'ref_name': d['ref_name'],
        #         'ref_desc': d['ref_desc'],
        #         'ref_links': d['ref_links'],
        #     } for d in out_dict_data
        # ]

        outfile.write(json.dumps(out_dict_data, indent=2, ensure_ascii=False))
