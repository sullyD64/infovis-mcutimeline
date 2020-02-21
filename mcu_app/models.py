# mcu_app/models.py
import json

from django.db import models
from django.utils.safestring import mark_safe
from pygments import highlight
from pygments.formatters.html import HtmlFormatter
from pygments.lexers.data import JsonLexer


class Source(models.Model):
    sid = models.CharField(max_length=8, primary_key=True)
    title = models.CharField(max_length=100, null=True)
    type = models.CharField(max_length=30)
    details = models.TextField()

    def details_formatted(self):
        json_obj = json.loads(self.details)
        data = json.dumps(json_obj, indent=2)
        formatter = HtmlFormatter(style='colorful')
        response = highlight(data, JsonLexer(), formatter)
        style = "<style>" + formatter.get_style_defs() + "</style><br/>"
        return mark_safe(style + response)

    def __str__(self):
        return self.sid


class Event(models.Model):
    eid = models.CharField(max_length=8, primary_key=True)
    filename = models.CharField(max_length=30)
    line = models.CharField(max_length=30)
    date = models.CharField(max_length=100)
    reality = models.CharField(max_length=30)
    title = models.CharField(max_length=200, null=True)
    desc = models.TextField()
    multiple = models.BooleanField(default=False)

    sources = models.ManyToManyField(Source, related_name='events')
   
    # characters = models.TextField(blank=True) # TODO characters
    # non_characters = models.TextField()
    # ref_special # TODO ref_special

    def __str__(self):
        return self.eid


class Ref(models.Model):
    rid = models.CharField(max_length=8, primary_key=True)
    name = models.CharField(max_length=30, null=True)
    desc = models.TextField()

    source = models.ForeignKey(Source, on_delete=models.CASCADE, related_name='refs')

    def __str__(self):
        return self.rid


class Reflink(models.Model):
    lid = models.CharField(max_length=8, primary_key=True)
    evt = models.ForeignKey(Event, on_delete=models.CASCADE, related_name='reflinks')
    src = models.ForeignKey(Source, on_delete=models.CASCADE, related_name='reflinks')
    ref = models.ForeignKey(Ref, on_delete=models.CASCADE, related_name='reflinks')

    def __str__(self):
        return f'{self.lid} ({self.evt} - {self.src} - {self.ref})'
