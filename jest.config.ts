import type { JestConfigWithTsJest } from "ts-jest";

const config: JestConfigWithTsJest = {
  preset: "ts-jest",
  testEnvironment: "node",
  testMatch: ["**/tests/**/*.spec.ts"],
  transform: {
    "^.+\\.tsx?$": "ts-jest",
  },
};

export default config;
