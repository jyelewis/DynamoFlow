module.exports = {
  preset: "ts-jest",
  verbose: false,
  bail: false,
  collectCoverage: true,
  coveragePathIgnorePatterns: [".mock.ts"],
  moduleNameMapper: {
    "(.*)\\.js$": "$1",
  },
  setupFiles: ["<rootDir>/jest.globalSetup.ts"],
  maxWorkers: 4,
};
