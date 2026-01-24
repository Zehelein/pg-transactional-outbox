import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import prettier from 'eslint-plugin-prettier/recommended';
import jest from 'eslint-plugin-jest';
import importPlugin from 'eslint-plugin-import';
import globals from 'globals';

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  prettier,
  {
    ignores: ['**/dist/**', '**/node_modules/**', '**/*.js', '**/*.mjs', '**/*.cjs'],
  },
  {
    files: ['**/*.ts', '**/*.tsx'],
    plugins: {
      import: importPlugin,
    },
    languageOptions: {
      ecmaVersion: 2018,
      sourceType: 'module',
      globals: {
        ...globals.browser,
        ...globals.node,
        ...globals.es2017,
      },
      parserOptions: {
        project: './tsconfig.json',
      },
    },
    settings: {
      'import/parsers': {
        '@typescript-eslint/parser': ['.ts'],
      },
      'import/resolver': {
        typescript: {
          alwaysTryTypes: true,
          project: ['libs/*/tsconfig.json', 'services/*/**/tsconfig.json'],
        },
      },
    },
    rules: {
      'prettier/prettier': 'error',
      '@typescript-eslint/ban-ts-comment': 'error',
      '@typescript-eslint/no-use-before-define': 'off',
      '@typescript-eslint/array-type': ['error', { default: 'array' }],
      'no-trailing-spaces': ['error', { ignoreComments: true }],
      'no-fallthrough': 'error',
      '@typescript-eslint/no-unused-vars': ['warn', { argsIgnorePattern: '^_' }],
      'spaced-comment': ['error', 'always'],
      '@typescript-eslint/no-explicit-any': 'warn',
      'no-console': 'warn',
      '@typescript-eslint/explicit-module-boundary-types': 'warn',
      '@typescript-eslint/explicit-function-return-type': 'off',
      '@typescript-eslint/consistent-type-definitions': ['error', 'interface'],
      'require-await': 'warn',
      curly: ['warn', 'all'],
      eqeqeq: ['error', 'allow-null'],
      'import/no-cycle': 'off', // TODO: Enabling this slows down VSCode ESLint plugin
    },
  },
  // Jest configuration for test files
  {
    files: ['**/*.{spec,test}.ts', '**/tests/**/*.ts', '**/__tests__/**/*.ts'],
    plugins: {
      jest,
    },
    languageOptions: {
      globals: {
        ...globals.jest,
      },
    },
    rules: {
      ...jest.configs.recommended.rules,
      ...jest.configs.style.rules,
      'jest/valid-expect': ['error', { maxArgs: 2 }],
      'jest/require-top-level-describe': 'error',
      'jest/no-commented-out-tests': 'warn',
      'no-console': 'off',
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/no-explicit-any': 'off',
      '@typescript-eslint/explicit-function-return-type': 'off',
    },
  },
);
