# data_scripts/tests.py
import unittest

from data_scripts import logconfig
from data_scripts.lib import constants, errors
from data_scripts.lib.logic import Scraper, Extractor
from data_scripts.lib.utils import TextFormatter

CODE = 'test'

class TextFormatterTest(unittest.TestCase):

    def setUp(self):
        self.cut = TextFormatter()

    def test_convert_bolds_to_html__simple(self):
        new_string = self.cut.text("'''simple'''").convert_bolds_to_html().get()
        self.assertEqual(new_string, "<b>simple</b>")

    def test_convert_bolds_to_html__one_single_quote_in_middle(self):
        new_string = self.cut.text("'''one'one'''").convert_bolds_to_html().get()
        self.assertEqual(new_string, "<b>one'one</b>")

    def test_convert_bolds_to_html__two_non_consecutive_single_quotes_in_middle(self):
        new_string = self.cut.text("'''two'non'consecutive'''").convert_bolds_to_html().get()
        self.assertEqual(new_string, "<b>two'non'consecutive</b>")

    def test_convert_bolds_to_html__two_replaces(self):
        new_string = self.cut.text("'''double''' abcdef '''double'''").convert_bolds_to_html().get()
        self.assertEqual(new_string, "<b>double</b> abcdef <b>double</b>")

    def test_convert_bolds_to_html__mixed_quotes_non_consecutive_in_middle(self):
        new_string = self.cut.text("'''mixed''one'and''two'''").convert_bolds_to_html().get()
        self.assertEqual(new_string, "<b>mixed''one'and''two</b>")
    
    def test_convert_bolds_to_html__no_replaces(self):
        new_string = self.cut.text("''no_replaces''").convert_bolds_to_html().get()
        self.assertEqual(new_string, "''no_replaces''")

    def test_convert_bolds_to_html__exception(self):
        self.cut.text("'''shouldraise''WikitagsNotBalancedError''")
        self.assertRaises(errors.WikitagsNotBalancedError, self.cut.convert_bolds_to_html)

    # strings = [
    #     "'''simple'''",  # 1
    #     "'''one'one'''",  # 2
    #     "'''two'non'consecutive'''",  # 3
    #     "'''two''consecutive'''",  # 4
    #     "'''double''' abcdef '''double'''",  # 5
    #     "'''three'non'con'secutive'''",  # 6
    #     "'''mixed''one'and''two'''",  # 7
    #     "''shouldntmatch''",  # 8
    #     "'''shouldraise''WikitagsNotBalancedError''",  # 9
    # ]
    # for i, s in enumerate(strings, start=1):
    #     new_string = None
    #     expected_num = 1
    #     if i == 5:
    #         expected_num = 2
    #     if i == 8 or i == 9:
    #         expected_num = 0 
        
    #     try:
    #         new_string = self.cut.text(s).convert_bolds_to_html().get()
    #         number_of_subs_made = len(re.findall('<b>', new_string))
    #         self.assertEqual(number_of_subs_made, expected_num, new_string)
    #         print(f'{s}\n{new_string}\n')
    #     except WikitagsNotBalancedError as e:
    #         if i == 9:
    #             print(e)
    #         else:
    #             raise Exception



class ParserTest(unittest.TestCase):

    def setUp(self):
        self.cut = Scraper(CODE)
        TEMPS_DIR = constants.PATH_INPUT_CRAWLED / 'wikitemplates'
        Extractor.code(CODE).cd(TEMPS_DIR)
        templates = {}
        temp_aff_path = next(TEMPS_DIR.glob('*__affiliation.json'), None)
        templates['affiliation'] = Extractor(infile=temp_aff_path).get_first()
        temp_cit_path = next(TEMPS_DIR.glob('*__citizenship.json'), None)
        templates['citizenship'] = Extractor(infile=temp_cit_path).get_first()
        
        Extractor.cd(constants.PATH_ROOT)
        Extractor.clean_output()
        self.templates = templates

    def test_crawl_character(self):
        self.cut.crawl_character(constants.PATH_ROOT, 'Catherine Wilder', self.templates)


if __name__ == "__main__":
    logconfig.config()
    unittest.main()
