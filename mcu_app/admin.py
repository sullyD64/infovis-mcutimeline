# mcu_app/admin.py
from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html
from django.contrib.contenttypes.models import ContentType

from mcu_app.models import Source, SourceHierarchy, Character, Event, Ref, Reflink

def linkify(field_name, many=False):
    """
    Converts a foreign key value into clickable links.
    https://stackoverflow.com/questions/37539132/

    If field_name is 'parent', link text will be str(obj.parent)
    Link will be admin url for the admin url for obj.parent.<model_key>:change
    """
    def _linkify(obj):
        linked_obj = getattr(obj, field_name)
        if not getattr(obj, field_name):
            return None
        c = ContentType.objects.get_for_model(obj)

        if not many:
            lc = ContentType.objects.get_for_model(linked_obj)
            view_name = f"admin:{c.app_label}_{lc.model}_change"
            link_url = reverse(view_name, args=[linked_obj.pk])
            return format_html('<a href="{}">{}</a>', link_url, linked_obj)
        else:
            if not linked_obj.all():
                return None

            lc = ContentType.objects.get_for_model(linked_obj.all()[0])
            view_name = f"admin:{c.app_label}_{lc.model}_change"
            m2m_linked_objs = [{
                'link_url': reverse(view_name, args=[obj.pk]), 
                'link_name': obj.pk,
                } for obj in linked_obj.all()]
            return format_html(', '.join([format_html('<a href="{}">{}</a>', obj['link_url'], obj['link_name']) 
                    for obj in m2m_linked_objs])
                )

    _linkify.short_description = field_name  # Sets column name
    return _linkify


class SourceAdmin(admin.ModelAdmin):
    def events_count(self, obj):
        return obj.events.count()
    fieldsets = [
        (None, {'fields': ['sid', 'parent', 'title', 'type']}),
        ('Details', {'fields': ['details_formatted']}),
    ]
    list_display = ('sid', linkify(field_name='parent'), 'title', 'type', 'details', 'events_count')
    readonly_fields = ('details', 'details_formatted')
    search_fields = ('sid', 'title',)


class SourceHierarchyAdmin(admin.ModelAdmin):
    fieldsets = [('Hierarchy', {'fields': ['hierarchy_formatted']})]
    readonly_fields = ('hierarchy', 'hierarchy_formatted')


class CharacterAdmin(admin.ModelAdmin):
    def events_count(self, obj):
        return obj.events.count()
    list_display = ('cid', 'cid_redirects', 'real_name', 'events_count', linkify(field_name='events', many=True))
    search_fields = ('cid', )

class EventAdmin(admin.ModelAdmin):
    list_display = ('eid', 'filename', 'date', 'reality', 'title',
        linkify(field_name='sources', many=True), 
        linkify(field_name='characters', many=True), 
        linkify(field_name='reflinks', many=True),
        'desc',
    )
    search_fields = ('eid', 'desc')


class RefAdmin(admin.ModelAdmin):
    list_display = ('rid', 'name', 'desc', 'source')
    search_fields = ('rid', )

class ReflinkAdmin(admin.ModelAdmin):
    list_display = ('lid', 
        linkify(field_name='evt'), 
        linkify(field_name='src'), 
        linkify(field_name='ref'),
    )
    search_fields = ('lid', )

admin.site.register(Source, SourceAdmin)
admin.site.register(SourceHierarchy, SourceHierarchyAdmin)
admin.site.register(Character, CharacterAdmin)
admin.site.register(Event, EventAdmin)
admin.site.register(Ref, RefAdmin)
admin.site.register(Reflink, ReflinkAdmin)
