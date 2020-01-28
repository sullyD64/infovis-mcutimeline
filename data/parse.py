import os
import re

from structs import Event, EventList
from utils import TextFormatter, MyHTMLParser

DIR = os.path.dirname(__file__)
IN_DIR = os.path.join(DIR, 'raw')
OUT_DIR = os.path.join(DIR, 'auto')

class Main(object):
    @staticmethod
    def parse(file: list, filename: str):
        elist = EventList()
        htmlparser = MyHTMLParser()

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
                    # remove_files_or_images:   done before processing text, because sometimes images are displayed on a single line.
                    # remove_empty_refs:        remove empty <ref> tags which were left over by the previous removal of links/images.
                    # fix_void_ref_nodes:       done before processing text, because those tags are incorrectly parsed by htmlparser and thus cause errors in detecting balanced tags.
                    # fix_incorrect_wikipedia_wiki_links: done before processing text, so that the wikitextparser doesn't recognise those links as wikilinks.
                    line = (TextFormatter()
                        .text(line)
                        .remove_wiki_images_or_files()
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

                    elist.events.append(ev)
        return elist

    @staticmethod
    def group_events(elist: EventList):
        events = elist.events
        gelist = EventList()
        main_ev = None
        sub_evs = []
        sub_evs_encountered = False

        for ev in events:
            level = int(ev.level)
            if level == 1:
                if sub_evs_encountered:
                    print(f'[Grouping] {main_ev.eid} with {[e.eid for e in sub_evs]}')
                    main_ev.join(sub_evs)
                    sub_evs = []
                    sub_evs_encountered = False
                main_ev = ev
                gelist.events.append(ev)
            elif level == 2:
                sub_evs.append(ev)
                sub_evs_encountered = True
            delattr(ev, 'level')
        return gelist


if __name__ == "__main__":
    outfile = f'{OUT_DIR}/parsed.json'

    filelist = ['before-20th', '1900s', '1910s', '1920s', '1930s', '1940s', '1950s', '1960s', '1970s', '1980s', '1990s', '2000s', '2010', '2011', '2012',
                '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2023', '2024', '2091', 'framework', 'lighthouse', 'dark-dimension', 'time-heist', ]
    # filelist = ['before-20th' ]
    # filelist = ['time-heist']
    # filelist = ['2018' ]

    elist = EventList()
    for i, fname in enumerate(filelist, start=1):
        try:
            infile = f'{IN_DIR}/{fname}'
            with open(infile) as wrapper:
                tfile = wrapper.read().splitlines()

                # COMMENT IF NEEDED
                print(f'[Parsing] {i}/{len(filelist)}\t{infile}')
                elist.events.extend(Main.parse(tfile, fname).events)

                # # UNCOMMENT IF NEEDED
                # # group nested events separatedly for each file
                # parsed_elist = parse(tfile, fname)
                # # print(f'[Parsing] {i}/{len(filelist)}\tgrouping nested events...')
                # grouped_elist = group_events(parsed_elist)

                # ## count events per file
                # print(f'{fname} #events {len(grouped_elist.events)}')
                # ## count events with titles per file
                # print(f'{fname} #titled {len(list(filter(lambda ev: ev.title, grouped_elist.events)))}')

                # elist.events.extend(grouped_elist.events)

        except FileNotFoundError:
            exit(f'[Error] file not found: "{fname}"')

    # COMMENT IF NEEDED
    # group nested events at the end
    print(f'[Parsing] grouping nested events...')
    elist.events = Main.group_events(elist).events

    with open(outfile, 'w') as output:
        output.write(str(elist))
        print(f'[Done] output available at {outfile}')
