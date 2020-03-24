# mcu_app/views.py
# import logging as log
import json

from django.http import JsonResponse
from django.shortcuts import render

from .processing import process_source_hierarchy
from .models import Event, Source, SourceHierarchy
from .serializers import EventSerializer

def index(request):
    sources = list(Source.objects.values())
    hierarchy = SourceHierarchy.objects.first().hierarchy
    context = {
        'sources_forest': process_source_hierarchy(sources, hierarchy)
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
