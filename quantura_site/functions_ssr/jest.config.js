module.exports = {
  testEnvironment: "node",
  testMatch: ["**/*.test.js"],
  collectCoverageFrom: ["index.js"],
  coveragePathIgnorePatterns: ["/node_modules/"],
  testTimeout: 10000,
};
