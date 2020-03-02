# data_scripts/lib/pipeline.py
import copy
import logging as log
import json
import re

import wikitextparser as wtp

from data_scripts.lib import constants, utils
from data_scripts.lib.structs import Event, Ref, Source, SourceBuilder, RefLink

class Actions():
    def __init__(self):
        self.counters = {}
        self.legends = {}

    def set_legends(self, **kwargs):
        """Allows to set variables or structures to be used ad updated during one or more Actions."""
        lgnds = {}
        for k, v in kwargs.items():
            lgnds[k] = v
        self.legends = lgnds

    def get_legend(self, lname):
        """Returns a variable or structure identified by its name."""
        return self.legends[lname]

    def set_counters(self, *args):
        cntrs = {}
        for v in args:
            cntrs[v] = 0
        self.counters = cntrs

    def get_counters(self):
        return self.counters

    # -----------------------------------

    def s1__iterate__events__group_nested(self, events: list):
        log.debug('Grouping nested events...')
        output = []
        main_ev = None
        sub_evs = []
        sub_evs_encountered = False

        for ev in events:
            level = int(ev.level)
            if level == 1:
                if sub_evs_encountered:
                    log.debug(f'Grouping {main_ev.eid} with {[e.eid for e in sub_evs]}')
                    main_ev.join(sub_evs)
                    sub_evs = []
                    sub_evs_encountered = False
                main_ev = ev
                output.append(ev)
            elif level == 2:
                sub_evs.append(ev)
                sub_evs_encountered = True
            delattr(ev, 'level')
        return output

    # -----------------------------------

    def s2__mapto__links__count_occurrences(self, link: str, **kwargs):
        oc = self.legends['occ_chars']
        onc = self.legends['occ_nonchars']

        scraper = kwargs['scraper']
        CHARS_DIR = kwargs['CHARS_DIR']
        quick = kwargs['quick']

        if link in onc.keys():
            onc[link] = (onc[link][0], onc[link][1] + 1) 
        elif (scraper.is_cached(CHARS_DIR, link) or (not quick and scraper.crawl_character(CHARS_DIR, link, kwargs['templates']))):
            oc[link] = (oc[link][0], oc[link][1] + 1) if link in oc.keys() else (0, 1)
        elif (not scraper.is_cached(CHARS_DIR, link) and (quick or not scraper.crawl_character(CHARS_DIR, link, kwargs['templates']))):
            onc[link] = (0,1) 
        else:
            raise Exception(f'FATAL: dead end {link}')

        max_chars = max(oc, key=lambda k: oc[k][1]) if oc else None
        max_chars_count = oc[max_chars][1] if max_chars else 0
        max_nonchars = max(onc, key=lambda k: onc[k][1]) if onc else None
        max_nonchars_count = onc[max_nonchars][1] if max_nonchars else 0
        tot = kwargs['tot']
        log.info(f'{self.legends["i"]}/{tot} (remaining: {tot-self.legends["i"]})')
        log.debug(f'unique characters: {len(oc)}')
        log.debug(f'unique noncharacters: {len(onc)}')
        log.debug(f'most frequent character: {max_chars} ({max_chars_count})')
        log.debug(f'most frequent noncharacter: {max_nonchars} ({max_nonchars_count})')
        self.legends["i"] += 1
        return link

    def s2__iterate__characters__group_alteregos(self, chars: list):
        log.debug('Grouping characters with alteregos...')
        oc = self.legends['occ_chars']
        output = []

        for c1 in chars:
            out = c1
            for c2 in chars:
                cid1, cid2 = c1['cid'], c2['cid']
                def get_plain(char):
                    return json.dumps({k: v for (k, v) in copy.deepcopy(char).items() if k not in ['cid', 'cid_redirects']})
                c1_plain = get_plain(c1)
                c2_plain = get_plain(c2)
                if (
                    c1_plain == c2_plain
                    # {k for k, _ in c1_plain.items() ^ c2_plain.items()} == {'cid'}
                    and cid1 != cid2
                ):
                    log.info(f'Grouping {cid1} ({oc[cid1][1]}) with {cid2} ({oc[cid2][1]})')
                    chars.remove(c2)
                    c1['cid_redirects'] = [cid2]
                    out = {**{
                        'cid': cid1,
                        'cid_redirects': [cid2]
                        }, **c1
                    }
                    oc[cid1] = (oc[cid1][0], oc[cid1][1] + oc[cid2][1])
                    oc.pop(cid2)
            output.append(out)
        return output

    def s2__mapto__linkoccurrences__initialize(self, occ: dict):
        return {k: (v, 0) for (k, v) in occ.items()}

    def s2__mapto__linkoccurrences__finalize(self, occ: dict):
        return {k: v[1] for (k, v) in sorted(occ.items())}

    def s2__mapto__linkoccurrences__rank(self, occ: dict):
        return {k: v for (k, v) in sorted(occ.items(), key=lambda item: item[1], reverse=True)}

    def s2__addattr__events__split_wikilinks(self, event_dict: dict, **kwargs):
        links = [wtp.parse(link).wikilinks[0].title for link in event_dict['links']]
        output = list(filter(lambda link: link in kwargs['valid'].keys(), links))
        return output

    def s2__mapto__characters__get_unique_sources(self, char_src: dict):
        sourcetitles_chars = self.legends['sourcetitles_chars']
        if char_src['source__title'] not in sourcetitles_chars.keys():
            sourcetitles_chars[char_src['source__title']] = char_src['source__type']
        return char_src

    # -----------------------------------

    def s3__iterate__sources__build(self, manual_sources: list):
        log.debug('Converting manual sources...')
        output = []
        for msrc in manual_sources:
            src = (SourceBuilder()
                .sid(msrc.pop('sid'))
                .title(msrc.pop('title'))
                .stype(constants.SRC_FILM if msrc['series_id'].startswith('F_') else constants.SRC_TV_EPISODE)
                .details(msrc)
                .build()
            )
            output.append(src)
        return output

    def s3__mapto__sources__extract_film_root_sources(self, src: Source):
        root_sources = self.legends['root_sources']
        if src.type == constants.SRC_FILM:
            rootsrc = (SourceBuilder()
                .sid(src.details['series_id'])
                .title(f'{src.details["series"]} (Film series)')
                .stype(constants.SRC_FILM_SERIES)
                .build()
            )
            if not rootsrc in root_sources:
                root_sources.append(rootsrc)
                log.debug(f'Found new film root source: {rootsrc.title}')
        return src

    def s3__mapto__sources__extract_tv_root_sources(self, src: Source):
        root_sources = self.legends['root_sources']
        if src.type == constants.SRC_TV_EPISODE:

            def get_season_title(title, number):
                switcher = {
                    1: 'One',
                    2: 'Two',
                    3: 'Three',
                    4: 'Four',
                    5: 'Five',
                    6: 'Six',
                }
                return f'{title}/Season {switcher[number]}'

            src_season_details = {k: v for k, v in src.details.items()}
            src_season_details.pop('episode')
            rootsrc_season = (SourceBuilder()
                .sid(src_season_details.pop('season_id'))
                .title(get_season_title(src_season_details['series'], src_season_details['season']))
                .stype(constants.SRC_TV_SEASON)
                .details(src_season_details)
                .build()
            )
            src_series_details = {k: v for k, v in src_season_details.items()}
            src_series_details.pop('season')
            rootsrc_series = (SourceBuilder()
                .sid(src_series_details['series_id'])
                .title(src_series_details['series'])
                .stype(constants.SRC_TV_SERIES)
                .build()
            )
            if src.sid.startswith('WHiH'):  # TODO workaround for WHiH
                src.type = constants.SRC_WEB_SERIES
                rootsrc_season.type = constants.SRC_WEB_SERIES
                rootsrc_series.type = constants.SRC_WEB_SERIES
            if rootsrc_series not in root_sources:
                root_sources.append(rootsrc_series)
                log.debug(f'Found new tv root source: {rootsrc_series.title}')
            if rootsrc_season not in root_sources:
                root_sources.append(rootsrc_season)
                log.debug(f'Found new tv root source: {rootsrc_season.title}')
        return src

    def s3__mapto__sources__extract_additional_sources(self, sources_chars: dict):
        sources = self.legends['sources']
        sources_updated = self.legends['sources_updated']
        sources_additional = self.legends['sources_additional']

        for fulltitle, stype in sources_chars.items():
            plaintitle, clarif = Source.split_titlestr(fulltitle)
            found_existing = False
            if (any([(src.title == fulltitle
                        or (src.plaintitle() == plaintitle
                            and src.clarification
                            and ((clarif and src.clarification.lower() == clarif.lower())
                                or (not clarif and src.clarification[1:-1].lower() == stype.lower())))
                    ) for src in [*sources, *sources_updated]])
                ):
                found_existing = True
                # log.debug(f'[_] Already existing "{fulltitle}"')
            else:
                for src in [*sources, *sources_updated]:
                    if (clarif
                        and src.is_updatable_with_new_clarification(clarif)
                        and src.plaintitle() == plaintitle
                    ):
                        found_existing = True
                        log.debug(f' ⮡  Updating {src.sid} "{src.title}" => "{fulltitle}"')
                        if src in sources:
                            sources.remove(src)
                            src.set_title(fulltitle)
                            if src not in sources_updated:
                                sources_updated.append(src)
                            self.counters['cnt_updated'] += 1
            if not found_existing:
                newsrc = (SourceBuilder()
                    .title(fulltitle)
                    .stype(stype)
                    .build()
                )
                if not newsrc in sources_additional:
                    sources_additional.append(newsrc)
                    log.debug(f'[X] Discovered new source: {newsrc.title} type: {newsrc.type}')
                    self.counters['cnt_discovered'] += 1
        return sources_chars

    def s3__addattr__refs__sourcetitle(self, ref: Ref, **kwargs):
        newattr = None
        if ref.desc:
            matches = re.match(re.compile(f'({"|".join(kwargs["patterns"])})'), ref.desc)
            if matches:
                text = matches.group(1)
                links = wtp.parse(text).wikilinks
                if links:
                    newattr = links[0].title
                    self.counters['cnt_existing'] += 1

                # special handling for named refs 
                if ref.name:
                    # simplify refname for named refs which have been matched with pattern 2
                    if re.match(re.compile(kwargs['patterns'][2]), ref.desc) and ref.name[-1].isdigit():
                        refname_before = ref.name
                        ref.name = re.sub(r'[0-9]', '', ref.name)
                        self.counters['cnt_updated'] += 1
                        log.debug(f'{ref.rid} Updating name {refname_before} => {ref.name}')
        return newattr

    def s3__addattr__anonrefs__sourceid(self, ref: Ref):
        sources = self.legends['sources']
        sources_updated = self.legends['sources_updated']
        sources_additional = self.legends['sources_additional']
        found = False
        newattr = None

        if hasattr(ref, 'source__id') and ref.source__id:
            newattr = ref.source__id
            self.counters['cnt_existing'] += 1
            return newattr

        fulltitle = ref.source__title
        plaintitle, clarif = Source.split_titlestr(fulltitle)

        for src in [*sources, *sources_updated]:
            if src.plaintitle() == plaintitle:
                log.debug(f'[X] {ref.rid} binding to {src.sid} "{plaintitle}"')
                found = True
                newattr = src.sid
                if clarif and src.is_updatable_with_new_clarification(clarif):
                    log.debug(f' ⮡  {ref.rid} Updating {src.sid} "{src.title}" => "{fulltitle}"')
                    if src in sources:
                        sources.remove(src)
                        src.set_title(fulltitle)
                        if src not in sources_updated:
                            sources_updated.append(src)
                        self.counters['cnt_updated'] += 1
                break
        if found:
            self.counters['cnt_existing'] += 1
        else:
            newsrc = (SourceBuilder()
                .title(fulltitle)
                .stype(constants.SRC_OTHER)
                .build()
            )
            sources_additional.append(newsrc)
            self.counters['cnt_discovered'] += 1
            log.debug(f'[_] {ref.rid} discovered new source: "{fulltitle}"')
        return newattr

    def s3__addattr__namedrefs__sourceid_1(self, ref: Ref):
        sources = self.legends['sources']
        sources_updated = self.legends['sources_updated']
        newattr = None
        for src in [*sources, *sources_updated]:
            if (
                src.sid == ref.name
                or (ref.source__title and src.title == ref.source__title)
            ):
                newattr = src.sid
                self.counters['cnt_existing'] += 1
                log.debug(f'[X] {ref.rid} binding to {src.sid} "{src.title}"')
                if (not src.sid
                    and src.title == ref.source__title
                    and src in sources
                    and not len(ref.name.split(' ')) > 1
                ):
                    log.debug(f' ⮡  {ref.rid} Updating source "{src.title}", adding sid "{ref.name}"')
                    sources.remove(src)
                    src.sid = ref.name
                    if src not in sources_updated:
                        sources_updated.append(src)
                        self.counters['cnt_updated'] += 1
        return newattr

    def s3__addattr__namedrefs__sourceid_2(self, ref: Ref):
        sources = self.legends['sources']
        sources_additional = self.legends['sources_additional']
        found_existing = False
        newattr = None

        if ref.source__id:
            newattr = ref.source__id
        else:
            for src in sources:
                if (
                    src.sid == ref.name
                    or (ref.source__title and src.title == ref.source__title)
                ):
                    newattr = src.sid
                    found_existing = True
                    self.counters['cnt_existing'] += 1
                    log.debug(f'[X] {ref.rid} binding to {src.sid} "{src.title}"')
            if not found_existing:
                tkns = ref.name.split(' ')
                log.debug(f'[_] {ref.rid} source not found: "{ref.name}"')
                if len(tkns) == 1:
                    newsrc = (SourceBuilder()
                        .sid(ref.name)
                        .title(ref.source__title)
                        .stype(constants.SRC_OTHER)
                        .build()
                    )
                    if tkns[0].startswith('SE2010'): # TODO special case (SE2010)
                        refname_before = ref.name
                        ref.name = ref.name[:-3]
                        log.debug(f' ⮡  {ref.rid} Updating name {refname_before} => {ref.name}')
                        newsrc.sid = ref.name
                        newsrc.title = "Stark Expo/Promotional Campaign"
                        newsrc.type = constants.SRC_WEB_SERIES

                    newattr = ref.name
                    if not newsrc in sources_additional:
                        sources_additional.append(newsrc)
                        self.counters['cnt_discovered'] += 1
                        log.debug(f' ⮡  {ref.rid} simple refname, discovered new source with title "{newsrc.title}"')
                else:
                    # simply mark the refs with a complex refname to iterate again after.
                    newattr = constants.TMP_MARKERS[1]
                    self.counters['cnt_ref_marked'] += 1
                    log.debug(f' ⮡  {ref.rid} complex refname, marked {constants.TMP_MARKERS[1]}')
        return newattr

    def s3__iterate__sources__group_comic_duplicates(self, sources: list):
        updated_sources_mapping = self.legends['updated_sources_mapping']
        output = []

        for this_src in sources:
            if not output or this_src.type not in [constants.SRC_COMIC, constants.SRC_OTHER]:
                output.append(this_src)
            else:
                main_src = next(iter(list(filter(lambda that_src: (
                    this_src.type == constants.SRC_OTHER
                    and that_src.type == constants.SRC_COMIC
                    and (this_src.sid[-1].isdigit() and this_src.sid[:-1] == that_src.sid)
                ), output))), None)

                if main_src:
                    log.debug(f'Found main source: {this_src.sid} => {main_src.sid}')
                    updated_sources_mapping[this_src.sid] = main_src.sid
                elif this_src not in output:
                    output.append(this_src)
        return output

    def s3__mapto__namedrefs__update_srcid_grouped(self, ref: Ref):
        updated_sources_mapping = self.legends['updated_sources_mapping']
        if ref.source__id and ref.source__id in updated_sources_mapping.keys():
            sourceid_before = ref.source__id
            ref.source__id = updated_sources_mapping[ref.source__id]
            ref.name = ref.source__id
            log.debug(f'{ref.rid} Updating refname and source__id {sourceid_before} => {ref.source__id}')
        return ref

    def s3__mapto__namedrefs__add_srcid_multiple(self, ref: Ref):
        sources = self.legends['sources']
        if ref.source__id == constants.TMP_MARKERS[1]:
            allsids = list(filter(lambda x: x is not None, [src.sid for src in sources]))
            found = False
            skip = False
            prfx = ref.name.split(' ')[0]
            if prfx in allsids:
                found = True
                ref.sources = [prfx]
            elif any([sid.startswith(prfx) for sid in allsids]):
                # TODO special case (PT)
                skip = True
                if prfx != 'PT':
                    raise Exception(f"{ref.rid} Found prfx in any startswith {ref.name}, should find none")

            if not found and not skip:
                if any(char in prfx for char in ['/', '-']):
                    sourcesfound = []
                    sub_prfxs_1 = prfx.split('/')
                    found_multiple = False
                    for sp in sub_prfxs_1:
                        if '-' in sp:
                            sub_prfxs_2 = sp.split('-')
                            start_ep = sub_prfxs_2[0]
                            if start_ep in allsids:
                                start_ep_src = list(filter(lambda src: src.sid == start_ep, sources))[0]
                                episodes = list(filter(lambda src: (
                                    src.details
                                    and 'season_id' in src.details
                                    and src.details['season_id'] == start_ep_src.details['season_id']
                                ), sources))
                                start = int(start_ep_src.details['episode'])
                                end = int(sub_prfxs_2[1])
                                found_multiple = True
                                sourcesfound = [*sourcesfound, *[ep.sid for ep in episodes[start-1:end]]]
                        else:
                            if sp in allsids:
                                found_multiple = True
                                sourcesfound.append(sp)
                    if found_multiple:
                        log.debug(f"{ref.rid} Multiple sources found based on the prefix: {ref.name}")
                        found = True
                        ref.sources = [*ref.sources, *sourcesfound]
            if found:
                self.counters['cnt__complex_ref_found'] += 1
                ref.source__id = constants.TMP_MARKERS[2]
            else:
                log.debug(f'{ref.rid} Not a source: {ref.name}')
                self.counters['cnt__not_a_source'] += 1
        return ref

    def s3__addattr__namedrefs__remaining_srctitle(self, ref: Ref):
        sources = self.legends['sources']
        newattr = None
        if ref.source__title:
            newattr = ref.source__title
        else:
            matching_src = next(iter(list(filter(lambda src: src.sid == ref.source__id, sources))), None)
            if matching_src:
                newattr = matching_src.title
        return newattr

    def s3__addattr__namedrefs__complex(self, ref: Ref):
        """
        The predicate after the 'or' is uses because before, when adding a source__title using matching patterns,
            (especially the pattern "In [source wikilink]...")
        some refs with complex names also received a source__id and then, when identifying
        if they were complex or not, they weren't marked as such (because they already had a source__id) and thus 
        at this point they don't have the constants.TMP_MARKERS[2] as source__id. 
        The predicate after the 'or' checks this particular case and fixes it.
        """
        return (ref.source__id == constants.TMP_MARKERS[2] 
            or ref.source__id != constants.TMP_MARKERS[1] and ref.name and len(ref.name.split(' ')) > 1
        )

    def s3__mapto__refs__convert_srcid_srctitle_to_sourcelist(self, ref: Ref):
        if len(ref.sources) == 0:
            ref.sources = [ref.source__id]
        return ref

    def s3__addattr__refs__create_events_list(self, ref: Ref):
        return [ref.event__id]

    def s3__mapto__refs__merge_remove_duplicates_1(self, this_ref: Ref):
        refs_nonvoid = self.legends['refs_nonvoid_0']
        refs_nonvoid_new = self.legends['refs_nonvoid_new_1']
        duplicate_refs = list(filter(lambda ref: (
            (ref == this_ref or
                (((not ref.name and this_ref.name) or (ref.name and not this_ref.name))
                 and ref.desc == this_ref.desc))
            and ref.rid != this_ref.rid
            and ref not in refs_nonvoid_new), refs_nonvoid))

        if duplicate_refs:
            for dref in duplicate_refs:
                this_ref.events.append(dref.event__id)
            log.debug(f'{this_ref.rid} found duplicates: {[ref.rid for ref in duplicate_refs]}')
            log.debug(f'{this_ref.rid} events list: {this_ref.events}')
            log.debug('')
            self.counters['cnt_found_duplicate_nonvoid'] += 1
        if this_ref not in refs_nonvoid_new:
            refs_nonvoid_new.append(this_ref)
        return this_ref

    def s3__mapto__refs__merge_remove_duplicates_2(self, this_ref: Ref):
        refs_nonvoid = self.legends['refs_nonvoid_1']
        main_ref = next(iter(list(filter(lambda ref: ref.name == this_ref.name, refs_nonvoid))), None)
        if main_ref:
            main_ref.events = sorted(list(set([*main_ref.events, *[this_ref.event__id]])))
            # propagate complex from duplicate to main ref
            main_ref.complex = any([main_ref.complex, this_ref.complex])
            self.counters['cnt_found_duplicate_void'] += 1
        return this_ref

    def s3__iterate__refs__merge_remove_duplicates_3(self, refs: list):
        output = []
        for this_ref in refs:
            if not output:
                output.append(this_ref)
            
            existing_ref = next(iter(list(filter(lambda that_ref: (
                this_ref.source == that_ref.source
                and all([this_ref.noinfo, that_ref.noinfo])
            ), output))), None)
            
            if existing_ref:
                existing_ref.events = sorted(list(set([*existing_ref.events, *this_ref.events])))
            elif not this_ref in output:
                output.append(this_ref)
        return output

    def s3__addattr__refs__noinfo(self, ref: Ref, **kwargs):
        matches = re.match(re.compile(f'({"|".join(kwargs["patterns"])})'), ref.desc)
        newattr = True if (matches and not ref.complex) else False
        return newattr

    def s3__mapto__refs__get_sources2events_m2m(self, ref: Ref):
        m2m = self.legends['sources2events_m2m']
        reflinks = self.legends['reflinks']

        for pair in [(ref.source, eid) for eid in ref.events]:
            if pair not in m2m:
                m2m.append(pair)

        if not ref.noinfo:
            for eid in ref.events:
                rl = RefLink(eid, ref.rid, ref.source)
                if rl not in reflinks:
                    reflinks.append(rl)
        return ref

    def s3__mapto__refs__clean_deleted_eids(self, ref: Ref):
        invalid_eids = self.legends['invalid_eids'] 
        new_events = []
        updated = False
        for eid in ref.events:
            if eid not in invalid_eids:
                new_events.append(eid)
                log.debug(f'{ref.rid} removing {eid}')
                updated = True
        if updated:
            self.counters['cnt_updated'] += 1
        ref.events = new_events
        return ref

    def s3__mapto__refs__get_event2ref_map(self, ref: Ref, **kwargs):
        e2rm = self.legends[kwargs['name']]
        for eid in ref.events:
            if eid in e2rm.keys() and e2rm[eid]:
                e2rm[eid] = sorted(list(set([*e2rm[eid], *[ref.rid]])))
            else:
                e2rm[eid] = [ref.rid]
        return ref

    def s3__addattr__events__sources_list(self, event: Event):
        m2m = self.legends['sources2events_m2m']
        newattr = []
        for pair in m2m:
            src, evt = pair[0], pair[1]
            if evt == event.eid:
                newattr.append(src)
                log.debug(f'{event.eid} adding source {src}')
        if not newattr:
            raise Exception('all events should have a source')
        log.debug(f'{event.eid} {newattr}')
        return newattr

    def s3__addattr__events__reflinks_list(self, event: Event):
        reflinks_by_evt = self.legends['reflinks_by_evt']
        newattr = []
        this_event_reflinks = next(iter(list(filter(lambda group: (
            group['evt'] == event.eid
        ), reflinks_by_evt))), None)

        if this_event_reflinks:
            newattr = [rl.lid for rl in this_event_reflinks['elements']]
            self.counters['cnt_existing'] += 1
            log.debug(f'{event.eid} binding to {newattr}')
        return newattr

    def s3__addattr__events__special_multisource_ref_id(self, event: Event, **kwargs):
        e2msr = self.legends[kwargs['name']]
        newattr = None
        if event.eid in e2msr.keys():
            newattr = next(iter(e2msr[event.eid]))
        return newattr

    def s3__addattr__sources__refids(self, src: Source):
        refs = self.legends['refs']
        newattr = []
        for ref in refs:
            if src.sid and src.sid == ref.source:
                newattr.append(ref.rid)
        return newattr

    def s3__addattr__sources__events_list(self, source: Source):
        m2m = self.legends['sources2events_m2m']
        newattr = []
        found = False
        for pair in m2m:
            src, evt = pair[0], pair[1]
            if src == source.sid:
                found = True
                newattr.append(evt)
                log.debug(f'{source.sid} adding event {evt}')

        if found:
            self.counters['cnt_existing_in_m2m'] += 1
            log.debug(f'{source.sid} binding to {newattr}')
        log.debug(f'{source.sid} {newattr}')
        return newattr

    def s3__addattr__sources__reflinks_list(self, source: Source):
        reflinks_by_src = self.legends['reflinks_by_src']
        newattr = []
        this_source_reflinks = next(iter(list(filter(lambda group: (
            group['src'] == source.sid
        ), reflinks_by_src))), None)

        if this_source_reflinks:
            newattr = [rl.lid for rl in this_source_reflinks['elements']]
            self.counters['cnt_existing_in_reflinks'] += 1
            log.debug(f'{source.sid} binding to {newattr}')
        return newattr

    def s3__iterate__sources__merge_remove_duplicates(self, sources: list):
        output = []
        for this_src in sources:
            if not output:
                output.append(this_src)

            existing_src = next(iter(list(filter(lambda that_src: (
                this_src.sid == that_src.sid
            ), output))), None)

            if existing_src:
                existing_src.title = next((x for x in [existing_src.title, this_src.title] if x is not None))
            elif not this_src in output:
                output.append(this_src)
        return output

    def s3__addattr__sources__level(self, src: Source):
        newattr = 0
        if src.details:
            if src.type == constants.SRC_TV_EPISODE:
                newattr = 2
            else:
                newattr = 1
        return newattr

    def s3__mapto__sources__hierarchy_level0(self, this_src: Source):
        hierarchy = self.legends['hierarchy']
        if this_src.level == 0:
            hierarchy.append(copy.deepcopy(this_src))
        return this_src

    def s3__mapto__sources__hierarchy_level1(self, this_src: Source):
        hierarchy = self.legends['hierarchy']
        if this_src.level == 1:
            root_src = next(iter(list(filter(lambda src: src.sid == this_src.details['series_id'], hierarchy))), None)
            if root_src:
                root_src.sub_sources = [*root_src.sub_sources, *[copy.deepcopy(this_src)]]
        return this_src

    def s3__mapto__sources__hierarchy_level2(self, this_src: Source):
        hierarchy = self.legends['hierarchy']
        if this_src.level == 2:
            root_src = next(iter(list(filter(lambda src: src.sid == this_src.details['series_id'], hierarchy))), None)
            if root_src:
                subroot_src = next(iter(list(filter(lambda src: src.sid == this_src.details['season_id'], root_src.sub_sources))), None)
                if subroot_src:
                    subroot_src.sub_sources = [*subroot_src.sub_sources, *[copy.deepcopy(this_src)]]
        return this_src

    
    def s3__addattr__sources__parent_source(self, src: Source):
        """Bottom-up hierarchy with links to parent source (faster)"""
        newattr = None
        if src.details:
            if 'season_id' in src.details:
                newattr = src.details['season_id']
            elif 'series_id' in src.details:
                newattr = src.details['series_id']
        return newattr


    def s3__mapto__chars__normalize_missing_attributes(self, char: dict):
        for key in constants.CHAR_SELECTED_ARGS:
            if key not in char.keys():
                char[key] = []
        char['real_name'] = char.pop('real name')
        return char

    def s3__addattr__sources__characters(self, src: Source, **kwargs):
        newattr = []
        for char in kwargs['allchars']:
            for app in char['appearences']:
                if (app['source__title'] == src.title
                    and (not 'notes' in app.keys()
                        or (app['notes']
                            and not any(substring in app['notes'] for substring in constants.CHAR_APPEARENCE_SKIP_WORDS)))
                ):
                    newattr.append({k: char[k] for k in ('cid', 'cid_redirects', 'real_name', 'num_occurrences')})
        log.debug(f'{src.sid} added {len(newattr)} characters')
        return newattr

    def s3__addattr__events__discover_characters(self, event: Event, **kwargs):
        sources = kwargs['sources']
        sources_index = kwargs['sources_index']
    
        chars_event_sources = []
        event_sources = [sources[sources_index[sid]] for sid in event.sources]
        for event_src in event_sources:
            while event_src:
                chars_event_sources.extend(event_src.characters)
                try:
                    event_src = sources[sources_index[event_src.parent]]
                except KeyError:
                    break
        chars_noduplicates = [i for n, i in enumerate(chars_event_sources) if i not in chars_event_sources[n + 1:]]
        chars = sorted(chars_noduplicates, key=lambda char: char['num_occurrences'], reverse=True)
        
        matching_chars = []
        tf = utils.TextFormatter()
        desc_toread = tf.text(event.desc).remove_wikilinks().remove_html_comments().get()
        for char in chars:
            cid_clean = (tf.text(char['cid'])
                .strip_clarification()
                .remove_html_comments()
                .get()
                .strip()
            )
            cid_redirects_clean = [tf.text(cr)
                .strip_clarification()
                .remove_html_comments()
                .get()
                .strip()
                for cr in char['cid_redirects']]
            real_names_clean = [tf.text(rn)
                .remove_ref_nodes()
                .strip_clarification()
                .remove_html_comments()
                .get()
                .strip() 
                for rn in char['real_name']]
            patterns = [
                cid_clean,
                cid_clean.split(' ')[-1],
                *cid_redirects_clean,
                *[cid.split(' ')[-1] for cid in cid_redirects_clean],
                *real_names_clean,
                *[rn.split(' ')[-1] for rn in real_names_clean],
            ]
            patterns = list(filter(lambda pattern: not pattern in constants.CHAR_NAME_SKIP_WORDS, patterns))
            matches = [pattern in desc_toread for pattern in patterns]
            if any(matches):
                matching_index = matches.index(True)
                log.debug(f'{event.eid} match "{patterns[matching_index]}"')
                for pattern in patterns: 
                    desc_toread = desc_toread.replace(pattern, '')
                # if char['cid'] not in event.characters:
                matching_chars.append(char)

        discovered_chars = []
        for char in matching_chars:
            if not any(cid in event.characters for cid in [char['cid'], *char['cid_redirects']]):
                discovered_chars.append(char['cid'])

        newattr = []
        if len(discovered_chars) > 0:
            self.counters['cnt_updated'] +=1
            log.info(f'[x] {event.eid} existing {len(event.characters)}, adding {len(discovered_chars)} {discovered_chars}')
            newattr = [*event.characters, *discovered_chars]
        else:
            log.debug(f'[_] {event.eid} no characters found')
            newattr = event.characters
        return newattr

    def s3__addattr__events__normalize_character_cids(self, event: Event, **kwargs):
        allchars = kwargs['allchars']
        allchars_index = kwargs['allchars_index']
        newattr = []
        for cid in event.characters:
            if cid in allchars_index.keys():
                newattr.append(cid)
            else:
                matching_char = next(iter(list(filter(lambda char: cid in char['cid_redirects'], allchars))), None)
                if not matching_char:
                    log.error(f'{event.eid} not found {cid}')
                    raise Exception
                newattr.append(matching_char['cid'])
                log.info(f'{event.eid} replacing {cid} => {matching_char["cid"]}')
        return newattr

    def s3__iterate__events__merge_consecutive_similar_events(self, events: list):
        log.info('Grouping consecutive similar events...')
        events_newid = self.legends['events_newid']
        output = []
        sub_evs = []
        main_ev = next(iter(events))
    
        for ev in events[1:]:
            if (
                main_ev.date == ev.date and
                main_ev.sources == ev.sources and
                main_ev.reality == ev.reality and
                any(char in ev.characters for char in main_ev.characters)
            ):
                events_newid[ev.eid] = main_ev.eid
                sub_evs.append(ev)
            else:
                if sub_evs:
                    log.info(f'Grouping {main_ev.eid} with {[e.eid for e in sub_evs]}')
                    main_ev.join(sub_evs)
                    sub_evs = []
                main_ev = ev
                output.append(ev)
        return output


    def s3__mapto__sources__update_eids(self, src: Source):
        events_newid = self.legends['events_newid']

        for eid in src.events:
            if eid in events_newid.keys():
                src.events[src.events.index(eid)] = events_newid[eid]
                log.debug(f'{src.sid:7} replacing {eid} => {events_newid[eid]}')
        return src


    def s3__mapto__reflinks__update_eids(self, rl: RefLink):
        events_newid = self.legends['events_newid']

        eid = rl.evt
        if eid in events_newid.keys():
            rl.evt = events_newid[eid]
            log.debug(f'{rl.lid} replacing {eid} => {rl.evt}')

        return rl
