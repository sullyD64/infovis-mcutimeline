# mcu_app/admin.py
from django.contrib import admin

from mcu_app.models import Source, Character, Event, Ref, Reflink

class SourceAdmin(admin.ModelAdmin):
    def events_count(self, obj):
        return obj.events.count()
    fieldsets = [
        (None, {'fields': ['sid', 'title', 'type']}),
        ('Details', {'fields': ['details_formatted']}),
    ]
    list_display = ('sid', 'parent', 'title', 'type', 'details', 'events_count')
    readonly_fields = ('details', 'details_formatted')


class CharacterAdmin(admin.ModelAdmin):
    def events_count(self, obj):
        return obj.events.count()
    list_display = ('cid', 'cid_redirects', 'real_name', 'events_count')


class EventAdmin(admin.ModelAdmin):
    def sources_count(self, obj):
        return obj.sources.count()
    def reflinks_count(self, obj):
        return obj.reflinks.count()
    list_display = ('eid', 'filename', 'date', 'reality', 'title', 'desc', 'sources_count', 'reflinks_count')
    

class RefAdmin(admin.ModelAdmin):
    list_display = ('rid', 'name', 'desc', 'source')


class ReflinkAdmin(admin.ModelAdmin):
    list_display = ('lid', 'evt', 'src', 'ref')

admin.site.register(Source, SourceAdmin)
admin.site.register(Character, CharacterAdmin)
admin.site.register(Event, EventAdmin)
admin.site.register(Ref, RefAdmin)
admin.site.register(Reflink, ReflinkAdmin)
