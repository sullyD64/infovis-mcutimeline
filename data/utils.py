import re
from html.parser import HTMLParser

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
