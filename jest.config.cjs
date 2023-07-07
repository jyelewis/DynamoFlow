module.exports = {
  verbose: false,
  bail: false,
  collectCoverage: true,
  coveragePathIgnorePatterns: [".mock.ts"],
  moduleNameMapper: {
    "(.*)\\.js$": "$1",
  },
  transform: {
    "^.+\\.(t|j)sx?$": "@swc/jest",
  },
  setupFiles: ["<rootDir>/jest.globalSetup.js"],
  maxWorkers: 4,
};
