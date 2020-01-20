import json
import os
import re
import sys
from html.parser import HTMLParser

import wikitextparser as wtp


class TextFormatter(object):

    def begin(self, text: str):
        self.text = text
        return self

    def end(self):
        return self.text

    # -------------- Line preprocessing

    def remove_wiki_images_or_files(self):
        """Remove file links and display image tags."""
        self.text = re.sub(r"\[\[[:]?File:([^\[\]]*\[\[[^\]]*\]\])*[^\]]*\]\]", '', self.text)
        return self

    def remove_empty_ref_nodes(self):
        """Removes ref tags with empty text or with just markup left."""
        self.text = re.sub(r'<ref[^>]*>[\']*</ref>', '', self.text)
        return self

    def fix_void_ref_nodes(self):
        """Fix incorrectly formatted void html elements."""
        self.text = re.sub(r'name=([^\'\"]*\'?[^\\\'\">]*)/>', r'name="\1" />', self.text)
        return self

    # -------------- Event desc preprocessing

    def remove_ref_nodes(self):
        """Remove ref nodes and all the text they contain. Use it to isolate description text."""
        self.text = re.sub(r'(?s)(<ref([^>]*[^\/])?>(.*?)<\/ref>|<ref[^>]*\/>)', '', self.text)
        return self

    # -------------- Event/Ref desc processing

    def strip_wiki_links(self):
        """Remove wikilink wrap. If a label is present, use the label instead of the page title."""
        self.text = re.sub(r'\[\[([^\|\]]*\|)?([^\]\|]*)\]\]', r'\2', self.text)
        return self

    def strip_wps_templates(self):
        """Remove wps template wrap. If a label is present, use the label instead of the page title."""
        self.text = re.sub(r'\{\{WPS(\|[^\}\}\|]*)?\|([^\}\}]*)\}\}', r'\2', self.text)
        return self

    def convert_ext_links_to_html(self):
        """Convert external links from wikitext format to html anchors."""
        self.text = re.sub(r'\[(http[^ ]*) ([^\]\[]*(\[[^\]]*\]*[^\]\[]*)*)\]', r'<a href="\1">\2</a>', self.text)
        return self

    def convert_bolds_to_html(self):
        """Wrap bold text with <b> tags."""
        self.text = re.sub(r"'''([^']*(')?[^']*)'''", r'<b>\1</b>', self.text)
        return self

    def convert_italics_to_html(self):
        """Wrap italic text with <i> tags."""
        self.text = re.sub(r"''([^']*(')?[^']*)''", r'<i>\1</i>', self.text)
        return self

    def remove_nowiki_html_tags(self):
        """Remove <nowiki> escape tags."""
        self.text = re.sub(r'<nowiki>([^>]*)</nowiki>', r'\1', self.text)
        return self

    # -------------- Ref desc processing

    def strip_ref_html_tags(self):
        """Removes starting and trailing <ref> tags"""
        self.text = re.sub(r'(?s)(<ref([^>]*[^\/])?>(.*?)<\/ref>|<ref[^>]*\/>)', r'\3', self.text)
        return self

    def convert_quotes_from_double_to_single(self):
        """Converts all double quotes to single quotes, to avoid collision in html attributes."""
        self.text = re.sub(r'\"', '\'', self.text)
        return self

    def convert_userbloglinks_to_html(self):
        """Convert internal wikilinks to user blog posts from wikitext format to html anchors."""
        urlprefix = 'https://marvelcinematicuniverse.fandom.com/wiki/'
        self.text = re.sub(r'\[\[User blog:([^\/]*)\/([^\|]*)\|([^\]]*)\]\]', r'<a href="https://marvelcinematicuniverse.fandom.com/wiki/User_blog:\1/\2">\3</a>', self.text)
        return self

    def remove_quote_templates(self):
        """Remove quote templates entirely."""
        self.text = re.sub(r'\{\{Quote(\|[^\|]*){3}\}\}', '', self.text)
        return self


class Ref(object):
    def __init__(self, eid: int, text: str = None):
        self.eid = eid
        if text:
            self.ref_name = MyHTMLParser().extract_name(text)
            self.ref_desc = (TextFormatter()
                .begin(text)
                .strip_ref_html_tags()
                .convert_quotes_from_double_to_single()
                .convert_userbloglinks_to_html()
                .strip_wiki_links()
                .strip_wps_templates()
                .remove_nowiki_html_tags()
                .remove_quote_templates()
                .convert_ext_links_to_html()
                .convert_bolds_to_html()
                .convert_italics_to_html()
                .end()
            )

            self.ref_desc = self.ref_desc if self.ref_desc else None  # replace empty string with none
            self.ref_links = [str(x) for x in wtp.parse(
                TextFormatter()
                .begin(text)
                .convert_userbloglinks_to_html()
                .strip_wps_templates()
                .remove_quote_templates()
                .end()
            ).wikilinks]

    def to_dict(self):
        return self.__dict__

    @classmethod
    def from_dict(self, **kwargs):
        ref = Ref(kwargs['eid'])
        ref.ref_name = kwargs['ref_name']
        ref.ref_desc = kwargs['ref_desc']
        ref.ref_links = kwargs['ref_links']
        return ref

    def __key(self):
        return (self.ref_name, self.ref_desc)

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, Ref):
            return self.__key() == other.__key()
        return NotImplemented
    
    def __lt__(self, other):
        if isinstance(other, Ref):
            return self.eid < other.eid
        return NotImplemented


class Event(object):
    eid = 0

    def __init__(self, filename: str = None, ln: str = None, text: str = None, day: str = None, month: str = None, year: str = None, reality: str = None, empty: bool = False):
        if empty:
            return

        Event.eid += 1
        self.id = Event.eid
        self.file = filename
        self.line = ln
        self.date = self.__get_date(day, month, year)
        self.reality = reality
        self.title = self.__get_title(text)

        tf = TextFormatter()
        text_norefs = (tf
            .begin(text)
            .remove_ref_nodes()
            .end()
        )

        self.desc = (tf
            .begin(text_norefs)
            .convert_quotes_from_double_to_single()
            .strip_wiki_links()
            .strip_wps_templates()
            .convert_ext_links_to_html()
            .convert_bolds_to_html()
            .convert_italics_to_html()
            .remove_nowiki_html_tags()
            .end()
        )

        self.level = self.__get_heading_level(self.desc)
        self.multiple = False

        self.links = [str(x) for x in wtp.parse(text_norefs).wikilinks]
        parsed = wtp.parse(text)
        # self.templates = [str(x) for x in parsed.templates]
        self.refs = [Ref(self.id, str(x)) for x in list(filter(self.__filter_tags, parsed.tags()))]  # only extract <ref> tags
        self.refs = list(filter(lambda x: any([x.ref_name, x.ref_desc]), self.refs))  # remove empty refs

    def join(self, sub_evs: list):
        start_line = re.sub(r'([0-9]+)-[0-9]*', '\1', self.line)
        self.line = f'{start_line}-{max([ev.line for ev in sub_evs])}'
        self.desc = self.desc + '\n' + '\n'.join([ev.desc for ev in sub_evs])
        self.multiple = True
        self.links = list(set(self.links).union(set([element for sublist in [ev.links for ev in sub_evs] for element in sublist])))
        # self.templates = list(set(self.templates).union(set([element for sublist in [ev.templates for ev in sub_evs] for element in sublist])))

        sub_evs_refs_flat = [element for sublist in [ev.refs for ev in sub_evs] for element in sublist]
        sub_evs_refs_unique = set(sub_evs_refs_flat)

        def change_eid(ref):
            ref.eid = self.id
            return ref
        self.refs = list(map(change_eid, list(set(self.refs).union(sub_evs_refs_unique))))

    def __get_date(self, day: str, month: str, year: str):
        date_str = ''
        if day:
            date_str = f'{month} {day}, {year}'
        else:
            if month:
                date_str = f'{month} {year}'
            else:
                date_str = year
        return date_str

    def __get_title(self, text):
        title = None
        match = re.search(r"('){3}\[\[[^\]]*\]\]('){3}", text)
        if match:
            title = match.group(0).strip("'[]")
        return title

    def __get_heading_level(self, text):
        heading_level = None
        if text[0] == '*' and text[1] != '*':
            heading_level = 1
        elif text[0:2] == '**':
            heading_level = 2
        return heading_level

    def __filter_tags(self, tag):
        ignored_tags = ['<br>', '<nowiki>', '<small>']
        return not any([str(tag).startswith(it) for it in ignored_tags])

    @classmethod
    def from_dict(self, **kwargs):
        ev = Event(empty=True)
        for k, v in kwargs.items():
            if k == 'refs':
                ev.refs = [Ref.from_dict(**x) for x in kwargs['refs']]
            else:
                exec(f'ev.{k} = kwargs["{k}"]')
        return ev

    def to_dict(self):
        '''
        WTP objects (which are just wrappers) are stringified on Event __init__,
        but Event still carries a list of Ref objects which are not JSON serializable.
        This method returns a normal __dict__ but ensures Ref objects in self.refs are dictified too.
        '''
        jdict = {}
        for k, v in self.__dict__.items():
            if v and isinstance(v, list) and all(isinstance(x, Ref) for x in v):
                v = [ref.to_dict() for ref in v]
            jdict[k] = v
        return jdict

    def __str__(self):
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)


class EventList(object):
    def __init__(self):
        self.events = []

    def __str__(self):
        dictlist = [ev.to_dict() for ev in self.events]
        return json.dumps(dictlist, indent=2, ensure_ascii=False)


class MyHTMLParser(HTMLParser):
    def handle_starttag(self, tag, attrs):
        if tag == 'ref':
            if hasattr(self, 'balanced'):
                self.balanced = not self.balanced
            if hasattr(self, 'name') and attrs:
                self.name = attrs[0][1]

    def handle_endtag(self, tag):
        if tag == 'ref' and hasattr(self, 'balanced'):
            self.balanced = not self.balanced

    def is_balanced(self, text):
        self.balanced = True
        self.feed(text)
        is_balanced = self.balanced
        # print('<ref> balanced: ', self.balanced)
        self.reset()
        return is_balanced

    def extract_name(self, text):
        self.name = None
        self.feed(text)
        return self.name


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
                    # fix_incorrect_refs:       done before processing text, because those tags are incorrectly parsed by htmlparser and thus cause errors in detecting balanced tags.
                    line = (TextFormatter()
                        .begin(line)
                        .remove_wiki_images_or_files()
                        .remove_empty_ref_nodes()
                        .fix_void_ref_nodes()
                        .end()
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
                    print(f'[Grouping] {main_ev.id} with {[e.id for e in sub_evs]}')
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
    indir = os.path.join(os.path.dirname(__file__), 'raw/')
    outpath = os.path.join(os.path.dirname(__file__), 'parsed.json')

    # if len(sys.argv) != 2:
    #     exit("usage: python parse.py <filepath>")
    # fname = sys.argv[1]

    filelist = ['before-20th', '1900s', '1910s', '1920s', '1930s', '1940s', '1950s', '1960s', '1970s', '1980s', '1990s', '2000s', '2010', '2011', '2012',
                '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2023', '2024', '2091', 'framework', 'lighthouse', 'dark-dimension', 'time-heist', ]
    # filelist = ['before-20th' ]
    # filelist = ['time-heist']
    # filelist = ['2018' ]

    elist = EventList()
    for i, fname in enumerate(filelist, start=1):
        try:
            with open(indir + fname) as wrapper:
                tfile = wrapper.read().splitlines()

                # COMMENT IF NEEDED
                print(f'[Parsing] {i}/{len(filelist)}\t{indir + fname}')
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

    with open(outpath, 'w') as outfile:
        outfile.write(str(elist))
        print(f'[Done] output available at {outpath}')
