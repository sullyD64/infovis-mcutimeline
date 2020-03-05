// webpack.config.js
// Production configuration for webpack

const BundleTracker = require('webpack-bundle-tracker');
const webpack = require('webpack');
const autoprefixer = require('autoprefixer');
const { CleanWebpackPlugin } = require('clean-webpack-plugin');

module.exports = {
  mode: 'production',
  entry: {
    mcu_frontend: `${__dirname}/mcu_frontend/js/index.js`,
  },
  output: {
    filename: '[name].[hash].bundle.js',
    path: `${__dirname}/mcu_frontend/dist/`,
    publicPath: '/mcu_frontend/',
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
