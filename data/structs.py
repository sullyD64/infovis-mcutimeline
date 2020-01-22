import json
import re
import wikitextparser as wtp

from utils import TextFormatter, MyHTMLParser


class Ref(object):
    def __init__(self, eid: int, text: str = None):
        self.eid = eid
        if text:
            self.ref_name = MyHTMLParser().extract_name(text)
            self.ref_desc = (TextFormatter()
                             .text(text)
                             .strip_ref_html_tags()
                             .mark_double_quotes()
                             .convert_ext_links_to_html()
                             .convert_userbloglinks_to_html()
                             .convert_bolds_to_html()
                             .convert_italics_to_html()
                             .strip_wiki_links()
                             .strip_wps_templates()
                             .remove_quote_templates()
                             .remove_nowiki_html_tags()
                             .restore_double_quotes()
                             .get()
                             )

            self.ref_desc = self.ref_desc if self.ref_desc else None  # replace empty string with none
            self.ref_links = [str(x) for x in wtp.parse(
                TextFormatter()
                .text(text)
                .convert_userbloglinks_to_html()
                .strip_wps_templates()
                .remove_quote_templates()
                .get()
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

    def key(self):
        return (self.ref_name, self.ref_desc)

    def __hash__(self):
        return hash(self.key())

    def __eq__(self, other):
        if isinstance(other, Ref):
            return self.key() == other.key()
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, Ref):
            return self.eid < other.eid
        return NotImplemented


class Event(object):
    eid = 0

    def __init__(
        self,
        filename: str = None,
        ln: str = None,
        text: str = None,
        day: str = None,
        month: str = None,
        year: str = None,
        reality: str = None,
        empty: bool = False):

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
                       .text(text)
                       .remove_ref_nodes()
                       .get()
                       )

        self.desc = (tf
                     .text(text_norefs)
                     .mark_double_quotes()
                     .convert_ext_links_to_html()
                     .convert_bolds_to_html()
                     .convert_italics_to_html()
                     .strip_wiki_links()
                     .strip_wps_templates()
                     .remove_nowiki_html_tags()
                     .restore_double_quotes()
                     .get()
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
        for k in kwargs.keys():
            if k == 'refs':
                ev.refs = [Ref.from_dict(**x) for x in kwargs['refs']]
            else:
                setattr(ev, k, kwargs[k])
                # exec(f'ev.{k} = kwargs["{k}"]')
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