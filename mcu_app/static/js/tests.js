
var margin = {
  top: window.innerHeight * 0.3,
  bottom: window.innerHeight * 0.4,
  right: 50,
  left: 50,
}

var height = window.innerHeight - margin.top - margin.bottom;
var screenWidth = window.innerWidth - margin.left - margin.right;


svg = d3.select('main')
  .append('svg')
  .attr('width', screenWidth + margin.left + margin.right)
  .attr('height', height + margin.top + margin.bottom)
  // .attr('class', 'bg-white')
  .append('g')  
  .attr('class', 'chart')
  .attr('transform', `translate(${margin.left}, ${margin.top})`)

  // .attr("viewBox","0 0 1 1")