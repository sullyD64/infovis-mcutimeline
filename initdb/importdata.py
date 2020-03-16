import json
import logging
from django.db import migrations

from data_scripts.lib import constants, errors
from data_scripts.lib.logic import Extractor

log = logging.getLogger(__name__)

OUTPUT = constants.PATH_OUTPUT
CODE = 'ImportDataDjango'

def importdata(apps, schema_editor):
    # log.setLevel(logging.DEBUG)
    Extractor.code(CODE).cd(OUTPUT)
    infile = next(OUTPUT.glob('*__final_*.json'), None)
    if not infile:
        raise errors.RequiredInputMissingError(CODE)

    log.info('1. SOURCES')
    extr_sources = (Extractor(infile=next(OUTPUT.glob('*__final_sources.json')))
        .remove_cols(['clarification', 'events', 'refs', 'reflinks'])
        .addattr('details', lambda src: json.dumps(src['details']))
    )
    sources = (extr_sources
        .fork()
        .remove_cols(['parent'])
        .get()
    )
    Source = apps.get_model('mcu_app', 'Source')
    Source.objects.all().delete()
    for i, src_dict in enumerate(sources, start=1):
        src_obj = Source(**src_dict)
        src_obj.save()
        log.debug(f'[{i}/{len(sources)}] {src_obj}')

    sources_parents = (extr_sources
        .fork()
        .select_cols(['sid', 'parent'])
        .filter_rows(lambda src_dict: src_dict['parent'])
        .get()
    )
    for i, src_dict in enumerate(sources_parents, start=1):
        src_obj_child = Source.objects.get(sid=src_dict['sid'])
        src_obj_parent = Source.objects.get(sid=src_dict['parent'])
        src_obj_child.parent = src_obj_parent
        src_obj_child.save()
        log.debug(f'[{i}/{len(sources_parents)}] Binding parent {src_obj_child} => {src_obj_parent}')


    log.debug('')
    log.info('2. CHARACTERS')
    characters = (Extractor(infile=next(OUTPUT.glob('*__final_allchars.json')))
        .addattr('voice_actor', lambda char_dict: char_dict['voice actor'])
        .remove_cols(['appearences', 'voice actor'])
        .get()
    )
    Character = apps.get_model('mcu_app', 'Character')
    Character.objects.all().delete()
    for i, char_dict in enumerate(characters, start=1):
        char_obj = Character(**char_dict)
        char_obj.save()
        log.debug(f'[{i}/{len(characters)}] {char_obj}')


    log.debug('')
    log.info('3. EVENTS')
    events = (Extractor(infile=next(OUTPUT.glob('*__final_events.json')))
        .addattr('filename', lambda evt: evt['file'])
        .remove_cols(['file', 'reflinks'])
        .remove_cols(['ref_special']) # TODO ref_special
        .get()
    )
    Event = apps.get_model('mcu_app', 'Event')
    Event.objects.all().delete()
    for i, evt_dict in enumerate(events, start=1):
        evt_sids = evt_dict.pop('sources')
        evt_cids = evt_dict.pop('characters')
        evt_obj = Event(**evt_dict)
        evt_obj.save()
        for sid in evt_sids:
            evt_obj.sources.add(Source.objects.get(sid=sid))
        for cid in evt_cids:
            evt_obj.characters.add(Character.objects.get(cid=cid))
        evt_obj.save()
        log.debug(f'[{i}/{len(events)}] {evt_obj} [sources: {evt_obj.sources.count()}, characters: {evt_obj.characters.count()}]')


    log.debug('')
    log.info('4. REFS')
    refs = (Extractor(infile=next(OUTPUT.glob('*__final_refs.json')))
       .remove_cols(['complex', 'noinfo', 'events'])
       .get()
    )
    Ref = apps.get_model('mcu_app', 'Ref')
    Ref.objects.all().delete()
    for i, ref_dict in enumerate(refs, start=1):
        sid = ref_dict.pop('source')
        ref_obj = Ref(**ref_dict)
        src_obj = Source.objects.get(sid=sid)
        ref_obj.source = src_obj
        ref_obj.save()
        log.debug(f'[{i}/{len(refs)}] {ref_obj} [{src_obj}]')


    log.debug('')
    log.info('5. REFLINKS')
    reflinks = (Extractor(infile=next(OUTPUT.glob('*__final_reflinks.json')))
        .get()
    )
    Reflink = apps.get_model('mcu_app', 'Reflink')
    Reflink.objects.all().delete()
    for i, rl in enumerate(reflinks, start=1):
        rl_obj = Reflink(rl['lid'])
        rl_obj.evt = Event.objects.get(eid=rl['evt'])
        rl_obj.src = Source.objects.get(sid=rl['src'])
        rl_obj.ref = Ref.objects.get(rid=rl['ref'])
        rl_obj.save()
        log.debug(f'[{i}/{len(reflinks)}] {rl_obj}')


    log.debug('')
    log.info('6. SOURCE HIERARCHY')
    source_hierarchy = (Extractor(infile=next(OUTPUT.glob('*__final_source_hierarchy.json')))
        .get()
    )
    SourceHierarchy = apps.get_model('mcu_app', 'SourceHierarchy')
    hierarchy_obj = SourceHierarchy(hierarchy=json.dumps(source_hierarchy))
    hierarchy_obj.save()


class Migration(migrations.Migration):

    dependencies = [
        ('mcu_app', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(importdata)
    ]
