# mcu_app/models.py
import json

from django.db import models
from django.utils.safestring import mark_safe
from pygments import highlight
from pygments.formatters.html import HtmlFormatter
from pygments.lexers.data import JsonLexer

def to_json_field(field):
    json_obj = json.loads(field)
    data = json.dumps(json_obj, indent=2)
    formatter = HtmlFormatter(style='colorful')
    response = highlight(data, JsonLexer(), formatter)
    style = "<style>" + formatter.get_style_defs() + "</style><br/>"
    return mark_safe(style + response)


class Source(models.Model):
    sid     = models.CharField(max_length=255, primary_key=True)
    title   = models.CharField(max_length=255, null=True)
    type    = models.CharField(max_length=255)
    details = models.TextField()
    parent  = models.ForeignKey('self', on_delete=models.CASCADE, related_name='sub_sources', null=True)

    def details_formatted(self):
        return to_json_field(self.details)

    def __str__(self):
        return self.sid


class SourceHierarchy(models.Model):
    hierarchy = models.TextField()

    def hierarchy_formatted(self):
        return to_json_field(self.hierarchy)


class Character(models.Model):
    cid             = models.CharField(max_length=255, primary_key=True)
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
    eid        = models.CharField(max_length=255, primary_key=True)
    filename   = models.CharField(max_length=255)
    line       = models.CharField(max_length=255)
    date       = models.CharField(max_length=255)
    reality    = models.CharField(max_length=255)
    title      = models.CharField(max_length=255, null=True)
    desc       = models.TextField()
    multiple   = models.BooleanField(default=False)
    sources    = models.ManyToManyField(Source, related_name='events')
    characters = models.ManyToManyField(Character, related_name='events')
    non_characters = models.TextField()
    # TODO ref_special = models.ForeignKey(...)

    def __str__(self):
        return self.eid


class Ref(models.Model):
    rid    = models.CharField(max_length=255, primary_key=True)
    name   = models.CharField(max_length=255, null=True)
    desc   = models.TextField()
    source = models.ForeignKey(Source, on_delete=models.CASCADE, related_name='refs')

    def __str__(self):
        return self.rid


class Reflink(models.Model):
    lid = models.CharField(max_length=255, primary_key=True)
    evt = models.ForeignKey(Event,  on_delete=models.CASCADE, related_name='reflinks')
    src = models.ForeignKey(Source, on_delete=models.CASCADE, related_name='reflinks')
    ref = models.ForeignKey(Ref,    on_delete=models.CASCADE, related_name='reflinks')

    def __str__(self):
        return f'{self.lid} ({self.evt} - {self.src} - {self.ref})'
