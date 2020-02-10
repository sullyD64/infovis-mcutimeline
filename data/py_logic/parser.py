# py_logic/parser.py
import logging as log
import re
from html.parser import HTMLParser

import wikitextparser as wtp

from py_logic.formatter import TextFormatter
from py_utils.constants import MEDIA_TYPES_APPEARENCE


class Parser(object):
    def parse_timeline(self, file: list, filename: str):
        from py_model.structs import Event

        parsed_events = []
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
                    """
                    remove_files_or_images:             done before processing text, because sometimes images are displayed on a single line.
                    remove_empty_refs:                  remove empty <ref> tags which were left over by the previous removal of links/images.
                    fix_void_ref_nodes:                 done before processing text, because those tags are incorrectly parsed by htmlparser and thus cause errors in detecting balanced tags.
                    fix_incorrect_wikipedia_wiki_links: done before processing text, so that the wikitextparser doesn't recognise those links as wikilinks.
                    """
                    line = (TextFormatter()
                            .text(line)
                            .remove_displayed_wiki_images_or_files_at_beginning()
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

                    parsed_events.append(ev)
        return parsed_events

    def parse_character(self, charid: str, chartemp: wtp.Template, templates: dict = None):
        tf = TextFormatter()
        output = {}
        args_toselect = ['real name', 'alias', 'species', 'citizenship', 'gender', 'age', 'DOB', 'DOD', 'status', 'title', 'affiliation', 'actor', 'voice actor']
        selected = list(filter(lambda arg: arg.name.strip() in args_toselect, chartemp.arguments))

        output['cid'] = charid

        for arg in selected:
            clean_text = (tf.text(arg.value.strip()).remove_templates().strip_wiki_links().strip_small_html_tags().get()).split('<br>')
            if len(clean_text) == 1:
                clean_text = clean_text[0]
            output[arg.name.strip()] = clean_text

        not_selected = list(filter(lambda arg: arg.name.strip() not in [*args_toselect, *MEDIA_TYPES_APPEARENCE], chartemp.arguments))
        for nsarg in not_selected:
            log.debug(f'Skipping arg "{nsarg.name.strip()}" : {nsarg.value.strip()}')

        appearences = []
        for media_arg in list(filter(lambda arg: arg.name.strip() in MEDIA_TYPES_APPEARENCE, chartemp.arguments)):
            if media_arg:
                for app in media_arg.value.split('<br>'):
                    app_parsed = wtp.parse(app)
                    if app_parsed.wikilinks:
                        app = {
                            'source__title': app_parsed.wikilinks[0].title,
                            'source__type': media_arg.name.strip(),
                        }
                        notes = tf.text(str(app_parsed.tags()[0])).strip_small_html_tags().strip_wiki_links().get() if app_parsed.tags() else None
                        if notes:
                            app['notes'] = notes[1:-1]
                        appearences.append(app)
        output['appearences'] = appearences

        def resolve_template(tname: str):
            if tname in output.keys():
                updated = []
                if not isinstance(output[tname], list):
                    output[tname] = [output[tname]]
                for el in output[tname]:
                    if el.split(' ')[0] in templates[tname].keys():
                        updated.append(f"{templates[tname][el.split(' ')[0]]} {''.join(el.split(' ')[1:])}".strip())
                    else:
                        updated.append(el)
                output[tname] = updated

        for tname in templates.keys():
            resolve_template(tname)

        return output


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
