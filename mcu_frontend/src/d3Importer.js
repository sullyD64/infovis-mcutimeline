// Modularly import only the D3.js modules you require
// https://gist.github.com/oscarmorrison/efa6f1213cc7bc5f410993d4139f0007
import { ascending } from 'd3-array';
import { json } from 'd3-fetch';
import * as d3Selection from 'd3-selection';

// This module extends d3-selection with helpful functions which are similar to the jQuery API
// https://webkid.io/blog/replacing-jquery-with-d3/
import 'd3-extended';

// This module adds multi-value syntax to selections and transitions, allowing you to set multiple
// attributes, styles or properties simultaneously with more concise syntax.
import 'd3-selection-multi';

export default {
  ascending,
  json,
  select: d3Selection.select,
  selectAll: d3Selection.selectAll,
};
