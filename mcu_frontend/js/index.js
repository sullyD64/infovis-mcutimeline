import '../scss/style.scss';
import d3 from './d3Importer';
import {
  DEBUG, MARGIN, SCREENHEIGHT, SCREENWIDTH,
} from './constants';
import Sidebar from './sidebar';

// Initialize SVG and logDump
const svg = d3.select('main')
  .append('svg')
  .attrs({
    width: SCREENWIDTH + MARGIN.left + MARGIN.right,
    height: SCREENHEIGHT + MARGIN.top + MARGIN.bottom,
  });

d3.select('main')
  .append('div')
  .attrs({
    id: 'logDump',
    class: `${DEBUG ? 'd-flex flex-column align-items-end mx-5' : 'd-none'}`,
  });

$(document).ready(() => {
  new Sidebar().loadControllers();
});

// TODO temporary
svg.attr('height', `${$('header').height()}px`);
// svg.append('g')
//   .attr('class', 'chart')
//   .attr('transform', `translate(${MARGIN.left}, ${MARGIN.top})`);

// svg.append('circle')
//   .attrs({
//     cx: SCREENWIDTH / 2,
//     cy: SCREENHEIGHT / 2,
//     r: 25,
//     class: 'fill-olive',
//   });
