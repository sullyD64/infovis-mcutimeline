# mcu_app/views.py
from django.shortcuts import render
from rest_framework import generics

from .models import Event
from .serializers import EventSerializer


def index(request):
    return render(request, 'index.html')


class EventView(generics.ListAPIView):
    queryset = Event.objects.all()
    serializer_class = EventSerializer