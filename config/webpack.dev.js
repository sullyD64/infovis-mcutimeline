// webpack.devserver.config.js
// Development configuration, used by webpack-dev-server

const merge = require('webpack-merge');
const path = require('path');
const common = require('./webpack.common.js');

const dir = path.resolve(__dirname, '../');

module.exports = merge(common, {
  mode: 'development',
  devtool: 'inline-source-map',
  output: {
    publicPath: 'http://localhost:3000/static/dist/',
  },
  devServer: {
    port: 3000,
    hot: true,
    headers: {
      'Access-Control-Allow-Origin': '*',
    },
    watchOptions: {
      ignored: /node_modules/,
    },
    contentBase: `${dir}/mcu_frontend`,
    watchContentBase: false,
  },
});
