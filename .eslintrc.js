module.exports = {
  root: true,
  extends: [
    './node_modules/js-dev-knife/lib/dev-tools/eslint-config/index',
  ],
  rules: {
    "no-console": "off"
  },
  overrides: [
    {
      files: ['**/src/test/**/*.js'],
      rules: {
        'import/no-default-export': 'off',
      }
    }
  ]
};
