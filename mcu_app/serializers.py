from rest_framework import serializers
from .models import Event

class EventSerializer(serializers.ModelSerializer):
    class Meta:
        model = Event
        fields = ('eid', 'filename', 'line', 'date', 'reality', 'title', 'desc', 'multiple', 'sources')