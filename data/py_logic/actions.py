# py_logic/actions.py
import copy
import logging as log
import re

import wikitextparser as wtp

from py_model.structs import Event, Ref, Source, SourceBuilder
from py_utils.constants import (SRC_COMIC, SRC_FILM, SRC_FILM_SERIES,
                                SRC_OTHER, SRC_TV_EPISODE, SRC_TV_SEASON,
                                SRC_TV_SERIES, SRC_WEB_SERIES, TMP_MARKERS)


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
                c1_plain = {k: v for (k,v) in copy.deepcopy(c1).items() if not isinstance(v, (list, dict))}
                c2_plain = {k: v for (k,v) in copy.deepcopy(c2).items() if not isinstance(v, (list, dict))}
                cid1, cid2 = c1['cid'], c2['cid']
                if (
                    {k for k, _ in c1_plain.items() ^ c2_plain.items()} == {'cid'}
                    and cid1 != cid2
                ):
                    log.info(f'Grouping {cid1} ({oc[cid1][1]}) with {cid2} ({oc[cid2][1]})')
                    chars.remove(c2)
                    c1['cid_redirects'] = [cid2]
                    out = {**{
                        'cid': c1_plain.pop('cid'),
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
        sources_chars = self.legends['sources_chars']
        if char_src['source__title'] not in sources_chars.keys():
            sources_chars[char_src['source__title']] = char_src['source__type']
        return char_src

    # -----------------------------------

    def s3__mapto__events__parse_raw(self, raw_event: dict):
        return Event.from_dict(**raw_event)

    def s3__iterate__sources__parse_raw(self, manual_sources: list):
        log.debug('Converting manual sources...')
        output = []
        for msrc in manual_sources:
            src = (SourceBuilder()
                .sid(msrc.pop('sid'))
                .title(msrc.pop('title'))
                .stype(SRC_FILM if msrc['series_id'].startswith('F_') else SRC_TV_EPISODE)
                .details(msrc)
                .build()
            )
            output.append(src)
        return output

    def s3__mapto__sources__extract_film_root_sources(self, src: Source):
        root_sources = self.legends['root_sources']
        if src.type == SRC_FILM:
            rootsrc = (SourceBuilder()
                .sid(src.details['series_id'])
                .title(f'{src.details["series"]} (Film series)')
                .stype(SRC_FILM_SERIES)
                .build()
            )
            if not rootsrc in root_sources:
                root_sources.append(rootsrc)
                log.debug(f'Found new film root source: {rootsrc.title}')
        return src

    def s3__mapto__sources__extract_tv_root_sources(self, src: Source):
        root_sources = self.legends['root_sources']
        if src.type == SRC_TV_EPISODE:

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
                .sid(src_season_details['season_id'])
                .title(get_season_title(src_season_details['series'], src_season_details['season']))
                .stype(SRC_TV_SEASON)
                .details(src_season_details)
                .build()
            )
            src_series_details = {k: v for k, v in src_season_details.items()}
            src_series_details.pop('season')
            rootsrc_series = (SourceBuilder()
                .sid(src_series_details['series_id'])
                .title(src_series_details['series'])
                .stype(SRC_TV_SERIES)
                .build()
            )
            if src.sid.startswith('WHiH'):  # TODO workaround for WHiH
                src.type = SRC_WEB_SERIES
                rootsrc_season.type = SRC_WEB_SERIES
                rootsrc_series.type = SRC_WEB_SERIES
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
                .stype(SRC_OTHER)
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
                        .stype(SRC_OTHER)
                        .build()
                    )
                    if tkns[0].startswith('SE2010'): # TODO special case (SE2010)
                        refname_before = ref.name
                        ref.name = ref.name[:-3]
                        log.debug(f' ⮡  {ref.rid} Updating name {refname_before} => {ref.name}')
                        newsrc.sid = ref.name
                        newsrc.title = "Stark Expo/Promotional Campaign"
                        newsrc.type = SRC_WEB_SERIES

                    newattr = ref.name
                    if not newsrc in sources_additional:
                        sources_additional.append(newsrc)
                        self.counters['cnt_discovered'] += 1
                        log.debug(f' ⮡  {ref.rid} simple refname, discovered new source with title "{newsrc.title}"')
                else:
                    # simply mark the refs with a complex refname to iterate again after.
                    newattr = TMP_MARKERS[1]
                    self.counters['cnt_ref_marked'] += 1
                    log.debug(f' ⮡  {ref.rid} complex refname, marked {TMP_MARKERS[1]}')
        return newattr

    def s3__iterate__sources__group_comic_duplicates(self, sources: list):
        updated_sources_mapping = self.legends['updated_sources_mapping']
        output = []

        for this_src in sources:
            if not output or this_src.type not in [SRC_COMIC, SRC_OTHER]:
                output.append(this_src)
            else:
                main_src = next(iter(list(filter(lambda that_src: (
                    this_src.type == SRC_OTHER
                    and that_src.type == SRC_COMIC
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
        if ref.source__id == TMP_MARKERS[1]:
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
                self.counters['cnt__secondary_ref_found'] += 1
                ref.source__id = TMP_MARKERS[2]
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

    def s3__addattr__namedrefs__is_secondary(self, ref: Ref):
        """
        The predicate after the 'or' is uses because before, when adding a source__title using matching patterns,
            (especially the pattern "In [source wikilink]...")
        some refs with complex names (which are secondary) also received a source__id and then, when identifying
        if they were complex or not, they weren't marked as such (because they already had a source__id) and thus 
        at this point they don't have the TMP_MARKERS[2] as source__id. 
        The predicate after the 'or' checks this particular case and fixes it.
        """
        return (ref.source__id == TMP_MARKERS[2] 
            or ref.source__id != TMP_MARKERS[1] and ref.name and len(ref.name.split(' ')) > 1
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
            main_ref.events = sorted([*main_ref.events, *[this_ref.event__id]])
            # propagate secondariety from duplicate to main ref
            main_ref.is_secondary = any([main_ref.is_secondary, this_ref.is_secondary])
            self.counters['cnt_found_duplicate_void'] += 1
        return this_ref

    def s3__iterate__refs__merge__remove_duplicates_3(self, refs: list):
        output = []
        for this_ref in refs:
            if not output:
                output.append(this_ref)
            
            existing_ref = next(iter(list(filter(lambda that_ref: (
                next(iter(this_ref.sources)) == next(iter(that_ref.sources))
                and this_ref.desc == that_ref.desc
            ), output))), None)
            
            if existing_ref:
                existing_ref.events = sorted([*existing_ref.events, *this_ref.events])
            elif not this_ref in output:
                output.append(this_ref)
        return output

    def s3__addattr__sources__refids(self, src: Source, **kwargs):
        refs = self.legends[f'refs_{kwargs["type"]}']
        newattr = []
        for ref in refs:
            if src.sid and src.sid in ref.sources:
                src_ref = ref.rid
                newattr.append(src_ref)
        return newattr

    def s3__addattr__sources__level(self, src: Source):
        newattr = 0
        if src.details:
            if src.type == SRC_TV_EPISODE:
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
            root_in_hierarchy = list(filter(lambda src: src.sid == this_src.details['series_id'], hierarchy))
            if root_in_hierarchy:
                root_src = root_in_hierarchy[0]
                root_src.sub_sources = [*root_src.sub_sources, *[copy.deepcopy(this_src)]]
        return this_src

    def s3__mapto__sources__hierarchy_level2(self, this_src: Source):
        hierarchy = self.legends['hierarchy']
        if this_src.level == 2:
            root_in_hierarchy = list(filter(lambda src: src.sid == this_src.details['series_id'], hierarchy))
            if root_in_hierarchy:
                root_src = root_in_hierarchy[0]
                subroot_in_hierarchy = list(filter(lambda src: src.sid == this_src.details['season_id'], root_src.sub_sources))
                if subroot_in_hierarchy:
                    subroot_src = subroot_in_hierarchy[0]
                    subroot_src.sub_sources = [*subroot_src.sub_sources, *[copy.deepcopy(this_src)]]
        return this_src
