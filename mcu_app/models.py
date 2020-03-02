# mcu_app/models.py
import json

from django.db import models
from django.utils.safestring import mark_safe
from pygments import highlight
from pygments.formatters.html import HtmlFormatter
from pygments.lexers.data import JsonLexer


class Source(models.Model):
    sid     = models.CharField(max_length=8, primary_key=True)
    title   = models.CharField(max_length=100, null=True)
    type    = models.CharField(max_length=30)
    details = models.TextField()
    parent  = models.ForeignKey('self', on_delete=models.CASCADE, related_name='sub_sources', null=True)

    def details_formatted(self):
        json_obj = json.loads(self.details)
        data = json.dumps(json_obj, indent=2)
        formatter = HtmlFormatter(style='colorful')
        response = highlight(data, JsonLexer(), formatter)
        style = "<style>" + formatter.get_style_defs() + "</style><br/>"
        return mark_safe(style + response)

    def __str__(self):
        return self.sid


class Character(models.Model):
    cid             = models.CharField(max_length=50, primary_key=True)
    cid_redirects   = models.TextField(null=True)
    real_name       = models.TextField(null=True)
    alias           = models.TextField(null=True)
    species         = models.TextField(null=True)
    gender          = models.TextField(null=True)
    age             = models.TextField(null=True)
    DOB             = models.TextField(null=True)
    DOD             = models.TextField(null=True)
    citizenship     = models.TextField(null=True)
    affiliation     = models.TextField(null=True)
    status          = models.TextField(null=True)
    title           = models.TextField(null=True)
    actor           = models.TextField(null=True)
    voice_actor     = models.TextField(null=True)
    num_occurrences = models.PositiveIntegerField()

    def __str__(self):
        return self.cid


class Event(models.Model):
    eid        = models.CharField(max_length=8, primary_key=True)
    filename   = models.CharField(max_length=30)
    line       = models.CharField(max_length=30)
    date       = models.CharField(max_length=100)
    reality    = models.CharField(max_length=30)
    title      = models.CharField(max_length=200, null=True)
    desc       = models.TextField()
    multiple   = models.BooleanField(default=False)
    sources    = models.ManyToManyField(Source, related_name='events')
    characters = models.ManyToManyField(Character, related_name='events')
    non_characters = models.TextField()
    # TODO ref_special = models.ForeignKey(...)

    def __str__(self):
        return self.eid


class Ref(models.Model):
    rid    = models.CharField(max_length=8, primary_key=True)
    name   = models.CharField(max_length=30, null=True)
    desc   = models.TextField()
    source = models.ForeignKey(Source, on_delete=models.CASCADE, related_name='refs')

    def __str__(self):
        return self.rid


class Reflink(models.Model):
    lid = models.CharField(max_length=8, primary_key=True)
    evt = models.ForeignKey(Event,  on_delete=models.CASCADE, related_name='reflinks')
    src = models.ForeignKey(Source, on_delete=models.CASCADE, related_name='reflinks')
    ref = models.ForeignKey(Ref,    on_delete=models.CASCADE, related_name='reflinks')

    def __str__(self):
        return f'{self.lid} ({self.evt} - {self.src} - {self.ref})'
