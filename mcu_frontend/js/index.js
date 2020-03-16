import '../scss/style.scss';
import d3 from './d3Importer';

import sidebar from './sidebar';

sidebar();

const margin = {
  top: window.innerHeight * 0.3,
  right: 50,
  bottom: window.innerHeight * 0.4,
  left: 50,
};
const screenHeight = window.innerHeight - margin.top - margin.bottom;
const screenWidth = window.innerWidth - margin.left - margin.right;

const svg = d3.select('main')
  .append('svg')
  .attrs({
    width: screenWidth + margin.left + margin.right,
    height: screenHeight + margin.top + margin.bottom,
  });
  // .append('g')
  // .attr('class', 'chart')
  // .attr('transform', `translate(${margin.left}, ${margin.top})`);

svg.attr('height', `${$('header').height()}px`);
// svg.append('circle')
//   .attrs({
//     cx: screenWidth / 2,
//     cy: screenHeight / 2,
//     r: 25,
//     class: 'fill-olive',
//   });


const inputData = [];

const csrftoken = $("input[name='csrfmiddlewaretoken']").val();
d3.json('/api/events_by_src/multi',
  {
    method: 'post',
    // credentials: 'same-origin',
    headers: {
      // Accept: 'application/json',
      // 'Content-Types': 'application/json',
      'X-CSRFToken': csrftoken,
    },
    body: JSON.stringify(['AoS101', 'AoS102']),
  })
  .then((data) => {
    data.forEach((element) => {
      d3.select('main').append('div').text(`${element.eid}, ${element.date}`);
      inputData.push(`${element.eid}, ${element.date}`);
    });
  })
  .catch((error) => { console.error(error); });

console.log(inputData);
