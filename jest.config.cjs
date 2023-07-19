module.exports = {
  verbose: false,
  bail: false,
  collectCoverage: true,
  coveragePathIgnorePatterns: [".mock.ts"],
  testPathIgnorePatterns: ["<rootDir>/dist/"],
  moduleNameMapper: {
    "(.*)\\.js$": "$1",
  },
  transform: {
    "^.+\\.(t|j)sx?$": ["@swc/jest", {
      // https://github.com/swc-project/swc/issues/3854
      jsc: {
        target: 'es2021',
      }
    }],
  },
  setupFiles: ["<rootDir>/jest.globalSetup.js"],
  maxWorkers: 4,
  coverageThreshold: {
    global: {
      branches: 100,
      functions: 100,
      lines: 100,
      // TODO: work out why istanbul doesn't like zod schema definitions
      statements: 99,
    },
  },
};
