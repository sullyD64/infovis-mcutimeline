import Loader from './loader';
import { DEBUG } from './constants';

class SourcesSettingsController {
  constructor(root) {
    this.root = $(root);
    this.tree = this.root.find('.tree-of-checkboxes');
    this.evtCount = this.root.find('.status > span').first().text(0);
    this.srcCount = this.root.find('.status > span').last().text(0);

    this.tree.find('li.has-children')
      .children('ul').hide();
    this.listen();

    this.loader = new Loader();
    this.selection = [];
    this.tree.find('input[type=checkbox]')
      .prop('checked', false)
      .each((i, el) => {
        if (JSON.parse(el.getAttribute('default'))) {
          el.checked = true;
          this.selection.push(el.id);
        }
      });
    this.defaultSelection = this.selection;
    this.update('load', this.selection);
  }

  static handleDisplay(node) {
    $(node).toggleClass('active')
      .next('ul')
      .slideToggle(300);
    $(node)
      .parent('li')
      .attr('aria-expanded', (i, elem) => (elem === 'true' ? 'false' : 'true'));
  }

  listen() {
    this.root.find('#sourcesSettingsSelectAll')
      .on('click', () => this.handleSelectAll());
    this.root.find('#sourcesSettingsReset')
      .on('click', () => this.handleReset());
    this.tree.find('li.has-children')
      .children('a')
      .on('click', (ev) => this.constructor.handleDisplay(ev.target));
    this.tree.find('input[type=checkbox]')
      .on('change', (ev) => this.handleSelected(ev.target));
  }

  pause() {
    this.root.toggleClass('busy');
    this.root.find('.status').toggleClass('active');
    this.root.find('*').off();
  }

  resume() {
    this.root.toggleClass('busy');
    this.root.find('.status').toggleClass('active');
    this.listen();
  }

  async update(mode, selection, legend = null) {
    this.pause();
    let len = 0;
    if (mode === 'load') {
      len = await this.loader.loadEvents(selection);
    } else if (mode === 'unload') {
      len = this.loader.unloadEvents(selection, legend);
    }
    this.evtCount.text(len);
    this.srcCount.text(this.selection.length);
    this.resume();
  }

  handleSelected(node) {
    const affected = [];
    affected.push(node.id);
    $(node)
      .siblings('ul')
      .find('input[type=checkbox]')
      .filter((i, child) => (node.checked ? !child.checked : child.checked))
      .each((i, child) => {
        child.checked = node.checked;
        affected.push(child.id);
      });

    if (node.checked) {
      const a = $(node).siblings('a');
      if (!a.hasClass('active') && $(node).parent('li').hasClass('has-children')) {
        this.constructor.handleDisplay(a);
      }
      this.selection = [...this.selection, ...affected];
      this.update('load', affected);
    } else {
      this.selection = this.selection.filter((id) => !affected.includes(id));
      this.update('unload', affected, this.selection);
    }
  }

  handleSelectAll() {
    const affected = [];
    this.tree.find('input[type=checkbox]:not(:checked)')
      .prop('checked', true)
      .each((i, el) => affected.push(el.id));
    this.update('load', affected);
    this.selection = [...this.selection, ...affected];
  }

  handleReset() {
    this.selection = this.defaultSelection;
    this.tree.find('input[type=checkbox]')
      .each((i, el) => {
        el.checked = this.defaultSelection.includes(el.id);
      });
    this.tree.find('li[aria-expanded=true]').children('a').click();
    this.loader.reset();
    this.update('load', this.selection);
  }
}

export default class Sidebar {
  constructor() {
    this.view = $('.sidebar');
    const overlay = $('.sitecontainer > .overlay');
    const hideButton = this.view.find('#hideSidebar');
    const showButton = $('#showSidebar');

    showButton.on('click', () => {
      this.view.add(overlay).addClass('active');
      this.view.find('li[aria-expanded=true]').attr('aria-expanded', 'false');
    });

    hideButton.add(overlay).on('click', () => {
      this.view.add(overlay).removeClass('active');
    });

    $(window).on('load resize', () => {
      const contentHeight = $(window).outerHeight()
        - this.view.find('header').outerHeight()
        - this.view.find('footer').outerHeight()
        - this.view.find('.hero').outerHeight();
      this.view.find('.content').outerHeight(contentHeight);
    });

    if (DEBUG) showButton.click();
  }

  loadControllers() {
    this.sourcesSettingsController = new SourcesSettingsController(
      this.view.find('.content > #sourcesSettings').get(0),
    );
  }
}
