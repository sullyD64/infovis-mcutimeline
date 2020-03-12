# mcu_app/views.py
import json
import logging as log
from data_scripts.lib import structs, utils
from data_scripts.lib.logic import Extractor

from django.http import JsonResponse
from django.shortcuts import render

from .models import Event, Source
from .serializers import EventSerializer

def index(request):

    # TODO: remove clarification from title
    # TODO: get hierarchy instead of flat source list
    # hierarchy level 0: source type
    # hierarchy level 1: root source
    # hierarchy level 2: season (optional)
    # hierarchy level 3: leaf source (movie/episode/issue)

    sources = list(Source.objects.values())

    extr_sources = (Extractor(data=sources)
        .parse_raw(structs.Source)
        .select_cols(['sid', 'parent_id', 'title'])
    )

    def remove_clarif(title):
        log.info(title)
        title = utils.TextFormatter().text(title).strip_clarification().get()
        return title

    titles = (extr_sources
        .consume_key('title')
        .mapto(remove_clarif)
        .get()
    )

    context = {
        'sources': titles
    }

    return render(request, 'index.html', context)


def handle_request_common(request, queryset, serializer_clazz):
    data = serializer_clazz(queryset, many=True).data
    return JsonResponse(data, safe=False)


def get_events_by_sid(request):
    sid = request.GET.get('source')
    qs = Event.objects.filter(sources=sid)
    return handle_request_common(request, qs, EventSerializer)

def get_events_by_sid_list(request):
    sids = json.loads(request.body.decode())
    qs = Event.objects.filter(sources__in=sids)
    return handle_request_common(request, qs, EventSerializer)
