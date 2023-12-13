import type { Config } from 'jest';

const config: Config = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ['**/*.spec.ts'],
  coveragePathIgnorePatterns: ['<rootDir>/dist/'],
  openHandlesTimeout: 2000, // Some internal timeouts that cannot be easily cancelled are 2sec long
};

export default config;
