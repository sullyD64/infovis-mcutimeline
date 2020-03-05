// webpack.devserver.config.js
// Development configuration, used by webpack-dev-server

const merge = require('webpack-merge');
const common = require('./webpack.config.js');

module.exports = merge(common, {
  mode: 'development',
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
    contentBase: `${__dirname}/mcu_frontend`,
    watchContentBase: false,
  },
});
