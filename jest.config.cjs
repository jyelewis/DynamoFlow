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
    "^.+\\.(t|j)sx?$": "@swc/jest",
  },
  setupFiles: ["<rootDir>/jest.globalSetup.js"],
  maxWorkers: 4,
  coverageThreshold: {
    global: {
      branches: 95,
      functions: 97,
      lines: 98,
      statements: 97,
    },
  },
};
