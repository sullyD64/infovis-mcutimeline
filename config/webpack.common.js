// webpack.config.js
// Production configuration for webpack

const BundleTracker = require('webpack-bundle-tracker');
const { CleanWebpackPlugin } = require('clean-webpack-plugin');
const autoprefixer = require('autoprefixer');
const path = require('path');
const webpack = require('webpack');

const dir = path.resolve(__dirname, '../');

module.exports = {
  mode: 'production',
  entry: {
    mcu_frontend: `${dir}/mcu_frontend/src/index.js`,
  },
  output: {
    filename: '[name].[hash].bundle.js',
    path: `${dir}/mcu_frontend/dist/`,
    publicPath: `${dir}/mcu_frontend/`,
  },
  module: {
    rules: [
      {
        test: /\.(sa|sc|c)ss$/,
        use: [
          { loader: 'style-loader' },
          { loader: 'css-loader' },
          {
            loader: 'postcss-loader',
            options: {
              plugins: [autoprefixer],
            },
          },
          { loader: 'sass-loader' },
        ],
      },
      {
        test: /.js$/,
        exclude: /node_modules/,
        use: 'babel-loader',
      },
      {
        test: /.*\.(png|jpe?g|gif|svg)$/,
        use: {
          loader: 'url-loader',
          options: {
            limit: 8192,
            name: '[path][name].[ext]',
          },
        },
      },
    ],
  },
  plugins: [
    new BundleTracker({ filename: './webpack-stats.json' }),
    new webpack.ProvidePlugin({ $: 'jquery' }),
    new CleanWebpackPlugin(),
  ],
  optimization: {
    splitChunks: {
      cacheGroups: {
        commons: {
          test: /[\\/]node_modules[\\/]/,
          chunks: 'initial',
          name: 'vendor',
        },
      },
    },
  },
};
