# data_scripts/lib/utils.py
import json
import re

from html.parser import HTMLParser
from data_scripts.lib import constants, errors

def jloads(rawfile):
    return json.loads(rawfile.read())

def jdumps(obj):
    return json.dumps(obj, indent=2, ensure_ascii=False)


def get_clarification(input_str: str):
    return next(iter(re.findall(r'(\([^\)]+\))$', input_str)), None)


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
        # log.debug(f'<ref> balanced: {self.balanced}')
        self.reset()
        return is_balanced

    def extract_name(self, text):
        self.name = None
        self.feed(text)
        return self.name


class TextFormatter(object):

    def text(self, text: str):
        self.t = text
        return self

    def get(self):
        return self.t

    # -------------- Line preprocessing

    def remove_displayed_wiki_images_or_files_at_beginning(self):
        """Removes file links and displayed images or files (thumbs) at the beginning of the line."""
        self.t = re.sub(r"^(\**)\[\[[:]?File:([^\[\]]*\[\[[^\]]*\]\])*[^\]]*\]\]", r'\1', self.t)
        return self

    def remove_empty_ref_nodes(self):
        """Removes ref tags with empty text or with just markup left."""
        self.t = re.sub(r'<ref[^>]*>[\']*</ref>', '', self.t)
        return self

    def fix_void_ref_nodes(self):
        """Fix incorrectly formatted void html elements."""
        self.t = re.sub(r'name=([^\'\"]*\'?[^\\\'\">]*)/>', r'name="\1" />', self.t)
        return self

    def fix_incorrect_wikipedia_wiki_links(self):
        """Converts wikilinks referring to wikipedia pages in wps templates."""
        self.t = re.sub(r'\[\[wikipedia:(([^\|\]]*)\|)?([^\]\|]*)\]\]', r'{{WPS|\1\3}}', self.t)
        return self

    # -------------- Event desc preprocessing

    def remove_ref_nodes(self):
        """Removes ref nodes and all the text they contain. Use it to isolate description text."""
        self.t = re.sub(r'(?s)(<ref([^>]*[^\/])?>(.*?)<\/ref>|<ref[^>]*\/>)', '', self.t)
        return self

    # -------------- Event/Ref desc processing

    def remove_displayed_wiki_images_or_files_everywhere(self):
        """Removes displayed images or files (thumbs) everywhere in the text."""
        self.t = re.sub(r"\[\[[:]?File:([^\[\]]*\[\[[^\]]*\]\])*[^\]]*\]\]", '', self.t)
        return self

    def strip_wiki_links(self):
        """Removes wikilink wrap. If a label is present, use the label instead of the page title."""
        self.t = re.sub(r'\[\[([^\|\]]*\|)?([^\]\|]*)\]\]', r'\2', self.t)
        return self

    def strip_wiki_links_files(self):
        """Removes wikilink wrap for files. If a label is present, use the label instead of the page title."""
        self.t = re.sub(r'\[\[[:]?File:([^\|\]]*\|)?([^\]\|]*)\]\]', r'\2', self.t)
        return self

    def strip_wps_templates(self):
        """Removes wps template wrap. If a label is present, use the label instead of the page title."""
        self.t = re.sub(r'\{\{WPS(\|[^\}\}\|]*)?\|([^\}\}]*)\}\}', r'\2', self.t)
        return self

    def convert_ext_links_to_html(self):
        """Converts external links from wikitext format to html anchors."""
        self.t = re.sub(r'\[(http[^ ]*) ([^\]\[]*(\[[^\]]*\]*[^\]\[]*)*)\]', r'<a href="\1">\2</a>', self.t)
        return self

    def convert_bolds_to_html(self):
        """Wraps bold text with <b> tags. Warning: always use it BEFORE convert_italics_to_html"""

        if len(re.split(r"'{3}", self.t)) % 2 == 0:
            raise errors.WikitagsNotBalancedError(self.convert_bolds_to_html.__name__, self.t)

        marded_text = re.sub(r"'{3}", constants.TMP_MARKERS[3], self.t)
        tkns = []
        balanced = True
        for c in marded_text:
            if c == constants.TMP_MARKERS[3]:
                if balanced:
                    tkns.append('<b>')
                else:
                    tkns.append('</b>')
                balanced = not balanced
            else:
                tkns.append(c)
        self.t = ''.join(tkns)
        return self

    def convert_italics_to_html(self):
        """Wraps italic text with <i> tags. Warning: always use it AFTER convert_bolds_to_html"""

        if len(re.split(r"'{2}", self.t)) % 2 == 0:
            raise errors.WikitagsNotBalancedError(self.convert_italics_to_html.__name__, self.t)

        marded_text = re.sub(r"'{2}", constants.TMP_MARKERS[4], self.t)
        tkns = []
        balanced = True
        for c in marded_text:
            if c == constants.TMP_MARKERS[4]:
                if balanced:
                    tkns.append('<i>')
                else:
                    tkns.append('</i>')
                balanced = not balanced
            else:
                tkns.append(c)
        self.t = ''.join(tkns)
        return self

    def remove_nowiki_html_tags(self):
        """Remove <nowiki> escape tags."""
        self.t = re.sub(r'<nowiki>([^>]*)</nowiki>', r'\1', self.t)
        return self

    # -------------- Ref desc processing

    def strip_ref_html_tags(self):
        """Removes starting and trailing <ref> tags"""
        self.t = re.sub(r'(?s)(<ref([^>]*[^\/])?>(.*?)<\/ref>|<ref[^>]*\/>)', r'\3', self.t)
        return self

    def mark_double_quotes(self):
        """Converts all double quotes to a special character, to avoid collision in html attributes."""
        self.t = re.sub(r'\"', constants.TMP_MARKERS[5], self.t)
        return self

    def restore_double_quotes(self):
        """Restores all "escaped" double quotes."""
        self.t = re.sub(constants.TMP_MARKERS[5], '"', self.t)
        return self

    def convert_userbloglinks_to_html(self):
        """Converts internal wikilinks to user blog posts from wikitext format to html anchors."""
        # urlprefix = 'https://marvelcinematicuniverse.fandom.com/wiki/'
        self.t = re.sub(r'\[\[User blog:([^\/]*)\/([^\|]*)\|([^\]]*)\]\]', r'<a href="https://marvelcinematicuniverse.fandom.com/wiki/User_blog:\1/\2">\3</a>', self.t)
        return self

    def remove_quote_templates(self):
        """Removes quote templates entirely. Warning: use it AFTER strip_wps_templates."""
        self.t = re.sub(r'\{{2}Quote[^\}]*\}{2}', '', self.t)
        return self

    # --------------

    def remove_templates(self):
        """Removes any template."""
        self.t = re.sub(r'\{\{[^\}]*(\|[^\}\}\|]*)?\|([^\}\}]*)\}\}', r'\2', self.t)
        return self

    def strip_small_html_tags(self):
        """"Removes starting and trailing <small> tags."""
        self.t = re.sub(r'<small>([^<>]*)</small>', r'\1', self.t)
        return self

    def uniformate_br_html_tags(self):
        """Converts all <br/> tags in <br>."""
        self.t = re.sub(r'<br/>', '<br>', self.t)
        return self

    def strip_clarification(self):
        """Removes (clarification) from end of string."""
        self.t = re.sub(r"(\([^\)]+\))$", '', self.t)
        return self

    def strip_html_comments(self):
        """Removes html comments"""
        self.t = re.sub(r"<!--.*?-->", '', self.t)
        return self