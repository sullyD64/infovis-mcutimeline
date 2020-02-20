from django.db import migrations

import json

from data_scripts.lib import constants, errors
from data_scripts.lib.logic import Extractor

OUTPUT = constants.PATH_OUTPUT
CODE = 'ImportDataDjango'

def importdata(apps, schema_editor):  
    Extractor.code(CODE).cd(OUTPUT)

    infile = next(OUTPUT.glob('*__final_*.json'), None)
    if not infile:
        raise errors.RequiredInputMissingError(CODE)

    # SOURCES
    sources = (Extractor(infile=next(OUTPUT.glob('*__final_sources.json')))
        .remove_cols(['clarification', 'events', 'refs', 'reflinks'])
        .addattr('details', lambda src: json.dumps(src['details']))
        .get()
    )
    Source = apps.get_model('mcu_app', 'Source')
    Source.objects.all().delete()
    for src_dict in sources:
        src_obj = Source(**src_dict)
        src_obj.save()

    # EVENTS
    events = (Extractor(infile=next(OUTPUT.glob('*__final_events.json')))
        .addattr('filename', lambda evt: evt['file'])
        .remove_cols(['file', 'reflinks'])
        .remove_cols(['characters', 'non_characters']) # TODO characters
        .remove_cols(['ref_special']) # TODO ref_special
        .get()
    )
    Event = apps.get_model('mcu_app', 'Event')
    Event.objects.all().delete()
    for evt_dict in events:
        evt_sources = evt_dict.pop('sources')
        evt_obj = Event(**evt_dict)
        evt_obj.save()
        for sid in evt_sources:
            evt_obj.sources.add(Source.objects.get(sid=sid))
        evt_obj.save()
    
    # REFS
    refs = (Extractor(infile=next(OUTPUT.glob('*__final_refs.json')))
       .remove_cols(['complex', 'noinfo', 'events'])
       .get()
    )
    Ref = apps.get_model('mcu_app', 'Ref')
    Ref.objects.all().delete()
    for ref_dict in refs:
        sid = ref_dict.pop('source')
        ref_obj = Ref(**ref_dict)
        src_obj = Source.objects.get(sid=sid)
        ref_obj.source = src_obj
        ref_obj.save()

    # REFLINKS
    reflinks = (Extractor(infile=next(OUTPUT.glob('*__final_reflinks.json')))
        .get()
    )
    Reflink = apps.get_model('mcu_app', 'Reflink')
    Reflink.objects.all().delete()
    for rl in reflinks:
        rl_obj = Reflink(rl['lid'])
        rl_obj.evt = Event.objects.get(eid=rl['evt'])
        rl_obj.src = Source.objects.get(sid=rl['src'])
        rl_obj.ref = Ref.objects.get(rid=rl['ref'])
        rl_obj.save()


class Migration(migrations.Migration):

    dependencies = [
        ('mcu_app', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(importdata)
    ]
