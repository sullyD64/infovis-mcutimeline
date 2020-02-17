# data_scripts/lib/errors.py

class WikitagsNotBalancedError(Exception):
    def __init__(self, method, string):
        super().__init__(f"[{method}] Cannot replace, wikitags aren't balanced.\n{string}")


class WikipageNotExistingError(Exception):
    def __init__(self):
        super().__init__(f'Wiki page not found.')


class ExtractorOutdirMissingError(Exception):
    def __init__(self):
        super().__init__(f'Extractor not correctly configured: outdir is missing.')


class RequiredInputMissingError(Exception):
    def __init__(self, code):
        super().__init__(f'Missing required input file(s) for script [{code}]')
