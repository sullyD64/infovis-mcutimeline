import Loader from './loader';
import { DEBUG } from './constants';

export default class Sidebar {
  constructor() {
    this.tree = $('#sidebarTree');
    this.counter = $('#sidebarTreeSelectedCount').text(0);
    this.loader = new Loader();
    this.selected = [];

    this.tree.find('input[type=checkbox]')
      .prop('checked', false)
      .each((i, el) => {
        if (JSON.parse(el.getAttribute('default'))) {
          el.checked = true;
          this.selected.push(el.id);
        }
      });
    this.default = this.selected;
    this.loader.loadEvents(this.selected);
    this.updateCounter();
  }

  listen() {
    const target = $('.sidebar, .overlay');
    if (DEBUG) target.toggleClass('active');
    $('#sidebarToggle').on('click', () => {
      target.addClass('active');
      $('a[aria-expanded=true]').attr('aria-expanded', 'false');
    });
    $('#sidebarDismiss, .overlay').on('click', () => {
      target.removeClass('active');
    });

    $(window).on('load resize', () => {
      const treeHeight = $(window).outerHeight()
        - $('.sidebar__header').outerHeight()
        - $('.sidebar__hero').outerHeight()
        - $('.sidebar__footer').outerHeight();
      $('.sidebar__content').outerHeight(treeHeight);
    });

    this.tree.find('li.has-children')
      .children('ul').hide()
      .siblings('a')
      .on('click', (ev) => this.constructor.handleDisplay(ev.target));
    $('#sidebarTreeSelectAll').on('click', () => this.handleSelectAll());
    $('#sidebarTreeReset').on('click', () => this.handleReset());

    this.tree.find('input[type=checkbox]')
      .on('change', (ev) => this.handleSelected(ev.target));
  }

  static handleDisplay(node) {
    $(node).toggleClass('active')
      .attr('aria-expanded', (i, elem) => (elem === 'true' ? 'false' : 'true'))
      .next('ul')
      .slideToggle(300);
  }

  updateCounter() {
    this.counter.text(this.selected.length);
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
      this.loader.loadEvents(affected);
      this.selected = [...this.selected, ...affected];
    } else {
      this.loader.unloadEvents(affected, this.selected);
      this.selected = this.selected.filter((id) => !affected.includes(id));
    }
    this.updateCounter();
  }

  handleSelectAll() {
    const affected = [];
    this.tree.find('input[type=checkbox]:not(:checked)')
      .prop('checked', true)
      .each((i, el) => affected.push(el.id));
    this.loader.loadEvents(affected);
    this.selected = [...this.selected, ...affected];
  }

  handleReset() {
    this.tree.find('input[type=checkbox]')
      .each((i, el) => {
        el.checked = this.default.includes(el.id);
      });
    this.selected = new Array(this.default);
    this.loader.reset().loadEvents(this.default);
  }
}
