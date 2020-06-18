import django
import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'mcu_project.settings')
django.setup()

if __name__ == "__main__":
    from mcu_app import models
    from mcu_app.processing import process_source_hierarchy
    from data_scripts.lib import constants
    from data_scripts.lib.logic import Extractor
    from data_scripts.lib import structs

    sources = list(models.Source.objects.values())
    hierarchy = models.SourceHierarchy.objects.first().hierarchy

    Extractor.cd(constants.PATH_OUTPUT).code('DJANGO').clean_output()
    Extractor(data=process_source_hierarchy(sources, hierarchy)).save('source_hierarchy')
    

    events = models.Event.objects.filter(sources=['AoS101'])
    print(list(events))

    Extractor(data=list(models.Event.objects.values())).parse_raw(structs.Event).save('parsed_events')
        
    # Extractor(data=list(models.Source.objects.values())).parse_raw(structs.Source).save('parsed_sources')
    # Extractor(data=list(models.Ref.objects.values())).parse_raw(structs.Ref).save('parsed_refs')
    # Extractor(data=list(models.Reflink.objects.values())).parse_raw(structs.Reflink).save('parsed_reflinks')
    # Extractor(data=list(models.Character.objects.values())).save('parsed_characters')
