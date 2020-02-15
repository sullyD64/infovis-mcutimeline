# py_utils/constants.py

TIMELINE_PAGES = ['before-20th', '1900s', '1910s', '1920s', '1930s', '1940s', '1950s', '1960s', '1970s', '1980s', '1990s', '2000s', '2010', '2011',
                  '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2023', '2024', '2091', 'framework', 'lighthouse', 'dark-dimension', 'time-heist', ]
TMP_MARKERS = {
    1: '\u03A0',  # Π
    2: '\u03A3',  # Σ
    3: '\u03A8',  # Ψ
    4: '\u03A9',  # Ω
    5: '\u03A6',  # Φ
}

SRC_TYPES = {
    'f': 'film',
    'fs': 'film series',
    'tve': 'tv episode',
    'tvse': 'tv season',
    'tvs': 'tv series',
    'ws': 'web series',
    'c': 'comic',
    'cs': 'comic series',
    'os': 'oneshot',
    'o': 'other',
}
SRC_FILM = SRC_TYPES['f']
SRC_FILM_SERIES = SRC_TYPES['fs']
SRC_TV_EPISODE = SRC_TYPES['tve']
SRC_TV_SEASON = SRC_TYPES['tvse']
SRC_TV_SERIES = SRC_TYPES['tvs']
SRC_WEB_SERIES = SRC_TYPES['ws']
SRC_COMIC = SRC_TYPES['c']
SRC_COMIC_SERIES = SRC_TYPES['cs']
SRC_ONESHOT = SRC_TYPES['os']
SRC_OTHER = SRC_TYPES['o']


BASE_WIKI_URL = 'https://marvelcinematicuniverse.fandom.com/wiki/'
MEDIA_TYPES_APPEARENCE = ['movie', 'oneshot', 'tv series', 'web series', 'comic']
# MEDIA_TYPES_APPEARENCE = ['movie', 'oneshot', 'tv series', 'web series', 'game', 'comic']
