# mcu_app/templatetags/tags.py
from django import template
from data_scripts.lib import constants, utils

register = template.Library()

@register.filter
def get_id(node: dict):
    if node['level'] == 0:
        node_id = node['val']
    else:
        node_id = node['val'].sid
    return node_id

@register.filter
def get_display_title(node: dict):
    if node['level'] == 0:
        title = constants.HIERARCHY_CATEGORY_MAP[node['val']]
    else:
        src = node['val']
        title = (utils.TextFormatter()
            .text(src.title)
            .strip_clarification()
            .get()
            .strip())

        if src.type == constants.SRC_FILM:
            title = f'<i>{title}</i> <small>{src.details["year"]}</small>'
        
        elif (src.type in [
                constants.SRC_COMIC, 
                constants.SRC_ONESHOT, 
                constants.SRC_WEB_SERIES,
                constants.SRC_OTHER,
                ]
            and node['level'] == 1):
            title = f'<i>{title}</i>'

        elif src.type in [constants.SRC_FILM_SERIES, constants.SRC_TV_SERIES]:
            title = f'<b>{title}</b>'

        elif (src.type == constants.SRC_TV_SEASON
            or src.type == constants.SRC_WEB_SERIES and node['level'] == 2):
            title = f'<b>Season {src.details["season"]}</b>'
        
        elif (src.type == constants.SRC_TV_EPISODE
            or src.type == constants.SRC_WEB_SERIES and node['level'] == 3):
            title = f'Ep{src.details["episode"]}: <i>{title}</i>'

    return title
