import '../scss/style.scss';
import d3 from './d3Importer';

$(document).ready(() => {
  // $('#sidebar').addClass('active');
  // $('.overlay').addClass('active');

  $('#sidebarDismiss, .overlay').on('click', () => {
    $('#sidebar').removeClass('active');
    $('.overlay').removeClass('active');
  });
  $('#sidebarToggle').on('click', () => {
    $('#sidebar').addClass('active');
    $('.overlay').addClass('active');
    $('.collapse.in').toggleClass('in');
    $('a[aria-expanded=true]').attr('aria-expanded', 'false');
  });

  const margin = {
    top: window.innerHeight * 0.3,
    right: 50,
    bottom: window.innerHeight * 0.4,
    left: 50,
  };
  const screenHeight = window.innerHeight - margin.top - margin.bottom;
  const screenWidth = window.innerWidth - margin.left - margin.right;

  const svg = d3.select('main').append('svg')
    .attrs({
      width: screenWidth + margin.left + margin.right,
      height: screenHeight + margin.top + margin.bottom,
    })
    .append('g')
    .attr('class', 'chart')
    .attr('transform', `translate(${margin.left}, ${margin.top})`);

  svg.append('circle')
    .attrs({
      cx: screenWidth / 2,
      cy: screenHeight / 2,
      r: 25,
      class: 'fill-olive',
    });
});
