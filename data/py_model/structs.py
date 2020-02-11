# py_model/structs.py
import re

import wikitextparser as wtp

from py_logic.formatter import TextFormatter
from py_logic.parser import MyHTMLParser
from py_utils.constants import SRC_FILM, SRC_TV_EPISODE, SRC_ONESHOT
from py_utils.helpers import jdumps


class Ref(object):
    rid = 0

    def __init__(
        self,
        event__id: int = None,
        text: str = None,
        empty: bool = False):

        if empty:
            return

        Ref.rid += 1
        self.rid = f'R{Ref.rid:05d}'
        self.event__id = event__id
        if text:
            self.name = MyHTMLParser().extract_name(text)
            self.desc = (TextFormatter()
                .text(text)
                .strip_ref_html_tags()
                .mark_double_quotes()
                .convert_ext_links_to_html()
                .convert_userbloglinks_to_html()
                .convert_bolds_to_html()
                .convert_italics_to_html()
                # .strip_wiki_links()
                .strip_wiki_links_files()
                .remove_displayed_wiki_images_or_files_everywhere()
                .strip_wps_templates()
                .remove_quote_templates()
                .remove_nowiki_html_tags()
                .restore_double_quotes()
                .get()
            )

            self.desc = self.desc if self.desc else None  # replace empty string with none
            self.links = [str(x) for x in wtp.parse(
                TextFormatter()
                .text(text)
                .convert_userbloglinks_to_html()
                .strip_wiki_links_files()
                .remove_displayed_wiki_images_or_files_everywhere()
                .strip_wps_templates()
                .remove_quote_templates()
                .get()
            ).wikilinks]

    def to_dict(self, **kwargs):
        if kwargs:
            pass # do nothing
        return self.__dict__

    @classmethod
    def from_dict(self, **kwargs):
        ref = Ref(empty=True)
        for k, v in kwargs.items():
            setattr(ref, k, v)
        return ref

    def key(self):
        return (self.name, self.desc)

    def __hash__(self):
        return hash(self.key())

    def __eq__(self, other):
        if isinstance(other, Ref):
            return self.key() == other.key()
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, Ref):
            return self.rid < other.rid
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
        self.eid = f'E{Event.eid:05d}'
        self.file = filename
        self.line = ln
        self.date = self.__get_date(day, month, year)
        self.reality = reality
        self.title = self.__get_title(text)

        tf = TextFormatter()
        text_norefs = (tf
            .text(text)
            .remove_ref_nodes()
            .remove_displayed_wiki_images_or_files_everywhere()
            .get()
        )

        self.desc = (tf
            .text(text_norefs)
            .mark_double_quotes()
            .convert_ext_links_to_html()
            .convert_bolds_to_html()
            .convert_italics_to_html()
            .strip_wiki_links()
            .strip_wiki_links_files()
            .remove_displayed_wiki_images_or_files_everywhere()
            .strip_wps_templates()
            .remove_nowiki_html_tags()
            .restore_double_quotes()
            .get()
        )

        self.level = self.__get_heading_level(self.desc)
        self.multiple = False

        self.links = list(set([str(x) for x in wtp.parse(text_norefs).wikilinks]))
        parsed = wtp.parse(text)
        # self.templates = [str(x) for x in parsed.templates]
        self.refs = [Ref(self.eid, str(x)) for x in list(filter(self.__filter_tags, parsed.tags()))]  # only extract <ref> tags
        self.refs = list(filter(lambda x: any([x.name, x.desc]), self.refs))  # remove empty refs

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
            ref.event__id = self.eid
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
                ev.refs = [Ref.from_dict(**x) for x in v]
            else:
                setattr(ev, k, v)
        return ev

    def to_dict(self, **kwargs):
        '''
        WTP objects (which are just wrappers) are stringified on Event __init__,
        but Event still carries a list of Ref objects which are not JSON serializable.
        This method returns a normal __dict__ but ensures Ref objects in self.refs are dictified too.
        '''
        jdict = {}
        for k, v in self.__dict__.items():
            if v and isinstance(v, list) and all(isinstance(x, Ref) for x in v) and (not kwargs or not kwargs['ignore_nested']):
                v = [ref.to_dict() for ref in v]
            jdict[k] = v
        return jdict

    def __str__(self):
        return jdumps(self.to_dict())


class Source(object):
    def __init__(
        self,
        sid: str = None,
        title: str = None,
        stype: str = None,
        details: dict = None,
        empty: bool = False):

        if empty:
            return

        self.sid = sid
        self.set_title(title)
        self.type = stype
        self.details = details

    def set_title(self, title: str):
        self.clarification = None
        if title:
            clarif = next(iter(re.findall(r'(\([^\)]+\))$', title)), None)
            # TODO workaround for Luke Cage 2.13: T.R.O.Y.
            if clarif and clarif != "(T.R.O.Y.)":
                self.clarification = clarif
        self.title = title

    def plaintitle(self):
        if self.clarification:
            return self.title[:-len(self.clarification)].strip()
        return self.title

    @classmethod
    def split_titlestr(self, titlestr: str):
        clarif = next(iter(re.findall(r'(\([^\)]+\))$', titlestr)), None)
        clarif = clarif if clarif != "(T.R.O.Y.)" else None
        newtitlestr = titlestr[:-len(clarif)].strip() if clarif else titlestr
        return (newtitlestr, clarif)

    def is_updatable_with_new_clarification(self, clarif: str):
        if (self.clarification
            or not self.details
            or not self.type in [SRC_FILM, SRC_TV_EPISODE, SRC_ONESHOT]
        ):
            return False
        clarif = clarif[1:-1] # remove parenthesis
        type_matches = clarif in self.type
        # TODO workaround for "episode" clarifications
        if clarif == 'episode':
            type_matches = True

        series_matches = False
        if 'series' in self.details.keys():
            series_matches = clarif in self.details['series']
        return any([type_matches, series_matches])

    @classmethod
    def from_dict(self, **kwargs):
        src = Source(empty=True)
        for k, v in kwargs.items():
            if k == 'sub_sources':
                src.sub_sources = [Source.from_dict(**x) for x in v]
            else:
                setattr(src, k, v)
        return src

    def to_dict(self, **kwargs):
        jdict = {}
        for k, v in self.__dict__.items():
            if v and isinstance(v, list) and all(isinstance(x, Source) for x in v) and (not kwargs or not kwargs['ignore_nested']):
                v = [src.to_dict() for src in v]
            jdict[k] = v
        return jdict

    def __str__(self):
        return jdumps(self.to_dict())

    def key(self):
        return (self.sid, self.title, self.type, self.details)

    def __hash__(self):
        return hash(self.key())

    def __eq__(self, other):
        if isinstance(other, Source):
            return self.key() == other.key()
        return NotImplemented


class SourceBuilder(object):
    def __init__(self):
        self.src = Source()

    def sid(self, sid: str):
        self.src.sid = sid
        return self

    def title(self, title: str):
        self.src.set_title(title)
        return self

    def stype(self, stype: str):
        self.src.type = stype
        return self

    def details(self, details: dict):
        self.src.details = details
        return self

    def build(self):
        return self.src