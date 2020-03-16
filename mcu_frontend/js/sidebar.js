const sidebar = () => {
  $(document).ready(() => {
    const $target = $('.sidebar, .overlay');
    $target.addClass('active');
    $('#sidebarDismiss, .overlay').on('click', () => {
      $target.removeClass('active');
    });
    $('#sidebarToggle').on('click', () => {
      $target.addClass('active');
      $('a[aria-expanded=true]').attr('aria-expanded', 'false');
    });

    const selected = new Set();
    const $st = $('#sidebarTree');
    const $count = $('#sidebarTreeSelectedCount');

    function toggleDisplay(el) {
      $(el).toggleClass('active')
        .attr('aria-expanded', (i, elem) => (elem === 'true' ? 'false' : 'true'))
        .next('ul')
        .slideToggle(300);
    }

    function updateCounter() {
      $count.text(selected.size);
    }

    function updateSelection(el, expand = true, recursive = true) {
      const { id } = el;
      const $ul = $(el).siblings('ul');
      const $a = $(el).siblings('a');

      if ($(el).is(':checked')) {
        selected.add(id);
        if (!$a.hasClass('active') && expand) {
          toggleDisplay($a);
        }
      } else {
        selected.delete(id);
      }

      if (recursive) {
        $ul.find('input:checkbox')
          .prop('checked', el.checked)
          .each((i, child) => updateSelection(child));
      }
    }

    function resetSelection() {
      $st.find('input:checkbox')
        .prop('checked', false);
      selected.clear();
      console.log(selected);
      updateCounter();
    }

    function initTree() {
      $st.find('li.has-children')
        .children('ul').hide()
        .siblings('a')
        .click((event) => toggleDisplay(event.currentTarget));
      $st.find('input:checkbox')
        .change((event) => {
          updateSelection(event.currentTarget);
          updateCounter();
          console.log(selected);
        });
      resetSelection();
    }
    initTree();

    function selectAll() {
      $st.find('input:checkbox:not(:checked)')
        .prop('checked', true)
        .each((i, child) => updateSelection(child, false, false));
      console.log(selected);
      updateCounter();
    }
    $('#sidebarTreeSelectAll').click(selectAll);
    $('#sidebarTreeReset').click(resetSelection);
  });

  $(window).on('load resize', () => {
    $('.sidebar__content').outerHeight($(window).outerHeight() - $('.sidebar__header').outerHeight() - $('.sidebar__footer').outerHeight() - $('.sidebar__hero').outerHeight());
  });
};

export default sidebar;
