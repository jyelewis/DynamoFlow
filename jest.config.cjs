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
      branches: 90,
      functions: 95,
      lines: 95,
      statements: 95,
    },
  },
};
