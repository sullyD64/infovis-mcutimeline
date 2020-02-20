# mcu_app/admin.py
from django.contrib import admin

from mcu_app.models import Source, Event, Ref, Reflink

class SourceAdmin(admin.ModelAdmin):
    def events_count(self, obj):
        return obj.event_set.count()
    fieldsets = [
        (None, {'fields': ['sid', 'title', 'type']}),
        ('Details', {'fields': ['details_formatted']}),
    ]
    list_display = ('sid', 'title', 'type', 'details', 'events_count')
    readonly_fields = ('details', 'details_formatted')


class EventAdmin(admin.ModelAdmin):
    def sources_count(self, obj):
        return obj.sources.count()
    def reflinks_count(self, obj):
        return obj.reflink_set.count()
    list_display = ('eid', 'filename', 'date', 'reality', 'title', 'desc', 'sources_count', 'reflinks_count')
    

class RefAdmin(admin.ModelAdmin):
    list_display = ('rid', 'name', 'desc', 'source')


class ReflinkAdmin(admin.ModelAdmin):
    list_display = ('lid', 'evt', 'src', 'ref')

admin.site.register(Source, SourceAdmin)
admin.site.register(Event, EventAdmin)
admin.site.register(Ref, RefAdmin)
admin.site.register(Reflink, ReflinkAdmin)
