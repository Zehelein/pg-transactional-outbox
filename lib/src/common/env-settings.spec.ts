import {
  BooleanSetting,
  Env,
  NumberSetting,
  StringSetting,
  constantToCamel,
  getConfigSettings,
  getConfigSettingsEnvTemplate,
  getEnvVariableBoolean,
  getEnvVariableNumber,
  getEnvVariableString,
} from './env-settings';

describe('Env Settings Unit Tests', () => {
  describe('Environment Variable String Utilities', () => {
    const mockEnv: Env = {
      MY_VAR: 'my-value',
      FALLBACK_VAR: 'fallback-value',
      EMPTY_VAR: '',
      UNDEFINED_VAR: undefined,
    };

    it('should retrieve an existing environment variable', () => {
      expect(getEnvVariableString(mockEnv, 'MY_VAR', 'FALLBACK_VAR')).toBe(
        'my-value',
      );
    });

    it('should retrieve an existing fallback environment variable', () => {
      expect(
        getEnvVariableString(mockEnv, 'NON_EXISTING', 'FALLBACK_VAR'),
      ).toBe('fallback-value');
    });

    it('should use a default value if the variable is empty', () => {
      expect(
        getEnvVariableString(mockEnv, 'EMPTY_VAR', 'FALLBACK_VAR', 'default'),
      ).toBe('default');
    });

    it('should throw an error if the variable is empty', () => {
      expect(() =>
        getEnvVariableString(mockEnv, 'EMPTY_VAR', 'FALLBACK_VAR'),
      ).toThrow(
        'The environment variable EMPTY_VAR must be a non-empty string.',
      );
    });

    it('should throw an error if the variable is not found', () => {
      expect(() =>
        getEnvVariableString(
          mockEnv,
          'NON_EXISTENT_VAR',
          'NON_EXISTENT_FALLBACK',
        ),
      ).toThrow(
        'The environment variable NON_EXISTENT_VAR must be a non-empty string.',
      );
    });

    it('should throw an error if the variable is undefined', () => {
      expect(() =>
        getEnvVariableString(mockEnv, 'UNDEFINED_VAR', 'NON_EXISTENT_FALLBACK'),
      ).toThrow(
        'The environment variable UNDEFINED_VAR must be a non-empty string.',
      );
    });
  });

  describe('Environment Variable Number Utilities', () => {
    const mockEnv: Env = {
      MY_NUMBER: '42',
      INVALID_NUMBER: 'not-a-number',
      EMPTY_NUMBER: '',
      UNDEFINED_NUMBER: undefined,
    };

    it('should retrieve a valid number from the environment variable', () => {
      expect(getEnvVariableNumber(mockEnv, 'MY_NUMBER', 'FALLBACK_VAR')).toBe(
        42,
      );
    });

    it('should use a default value if the variable is empty', () => {
      expect(
        getEnvVariableNumber(mockEnv, 'UNDEFINED_NUMBER', 'FALLBACK_VAR', 10),
      ).toBe(10);
    });

    it('should throw an error if the variable is not a valid number', () => {
      expect(() =>
        getEnvVariableNumber(mockEnv, 'INVALID_NUMBER', 'FALLBACK_VAR'),
      ).toThrow('The environment variable INVALID_NUMBER must be a number.');
    });

    it('should throw an error if the variable is empty', () => {
      expect(() =>
        getEnvVariableNumber(mockEnv, 'EMPTY_NUMBER', 'FALLBACK_VAR'),
      ).toThrow('The environment variable EMPTY_NUMBER must be a number.');
    });

    it('should throw an error if the variable is undefined', () => {
      expect(() =>
        getEnvVariableNumber(mockEnv, 'UNDEFINED_NUMBER', 'FALLBACK_VAR'),
      ).toThrow('The environment variable UNDEFINED_NUMBER must be a number.');
    });
  });

  describe('constantToCamel', () => {
    it('should convert constant case to camel case', () => {
      expect(constantToCamel('MY_CONSTANT_VALUE')).toBe('myConstantValue');
    });

    it('should handle empty input', () => {
      expect(constantToCamel('')).toBe('');
    });

    it('should handle single-word constant', () => {
      expect(constantToCamel('HELLO')).toBe('hello');
    });

    it('should handle non-alphanumeric characters', () => {
      expect(constantToCamel('MY_CONST_123')).toBe('myConst123');
    });

    it('should handle mixed case', () => {
      expect(constantToCamel('MY_Constant_Value')).toBe('myConstantValue');
    });
  });

  describe('getEnvVariableBoolean', () => {
    const mockEnv = {
      FIELD_TRUE: 'true',
      FIELD_FALSE: 'false',
      FIELD_ONE: '1',
      FIELD_ZERO: '0',
      FIELD_OTHER: 'other_value',
    };

    it('should return true for boolean values "true", "1"', () => {
      expect(getEnvVariableBoolean(mockEnv, 'FIELD_TRUE', 'FALLBACK_VAR')).toBe(
        true,
      );
      expect(getEnvVariableBoolean(mockEnv, 'FIELD_ONE', 'FALLBACK_VAR')).toBe(
        true,
      );
    });

    it('should return false for boolean values "false", "0"', () => {
      expect(
        getEnvVariableBoolean(mockEnv, 'FIELD_FALSE', 'FALLBACK_VAR'),
      ).toBe(false);
      expect(getEnvVariableBoolean(mockEnv, 'FIELD_ZERO', 'FALLBACK_VAR')).toBe(
        false,
      );
    });

    it('should throw an error for non-boolean values', () => {
      expect(() =>
        getEnvVariableBoolean(mockEnv, 'FIELD_OTHER', 'FALLBACK_VAR'),
      ).toThrow('The environment variable FIELD_OTHER must be a number.');
    });
  });

  describe('getConfigSettings', () => {
    const inboxSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] =
      [
        {
          constantName: 'STRING_VALUE',
          default: 'test-string',
          func: getEnvVariableString,
          description: 'my string',
        },
        {
          constantName: 'NUMBER_VALUE',
          default: 123,
          func: getEnvVariableNumber,
          description: 'my number',
        },
        {
          constantName: 'BOOLEAN_VALUE',
          default: true,
          func: getEnvVariableBoolean,
          description: 'my boolean',
        },
      ];

    it('should return config settings with all default values', () => {
      const expectedSettings = {
        stringValue: 'test-string',
        numberValue: 123,
        booleanValue: true,
      };

      const settings = getConfigSettings(
        inboxSettingsMap,
        'MAIN_',
        'FALLBACK_',
        {},
      );

      expect(settings).toEqual(expectedSettings);
    });

    it('should return config settings for the main values', () => {
      const mockEnv = {
        MAIN_STRING_VALUE: 'main-test',
        MAIN_NUMBER_VALUE: '456',
        MAIN_BOOLEAN_VALUE: 'false',

        FALLBACK_STRING_VALUE: 'fallback-test',
        FALLBACK_NUMBER_VALUE: '789',
        FALLBACK_BOOLEAN_VALUE: 'true',
      };
      const expectedSettings = {
        stringValue: 'main-test',
        numberValue: 456,
        booleanValue: false,
      };

      const settings = getConfigSettings(
        inboxSettingsMap,
        'MAIN_',
        'FALLBACK_',
        mockEnv,
      );

      expect(settings).toEqual(expectedSettings);
    });

    it('should return config settings for the fallback values', () => {
      const mockEnv = {
        FALLBACK_STRING_VALUE: 'fallback-test',
        FALLBACK_NUMBER_VALUE: '789',
        FALLBACK_BOOLEAN_VALUE: 'true',
      };
      const expectedSettings = {
        stringValue: 'fallback-test',
        numberValue: 789,
        booleanValue: true,
      };

      const settings = getConfigSettings(
        inboxSettingsMap,
        'MAIN_',
        'FALLBACK_',
        mockEnv,
      );

      expect(settings).toEqual(expectedSettings);
    });
  });

  describe('getConfigSettingsEnvTemplate', () => {
    const inboxSettingsMap: (StringSetting | NumberSetting | BooleanSetting)[] =
      [
        {
          constantName: 'STRING_VALUE',
          default: 'test-string',
          func: getEnvVariableString,
          description: 'my string',
        },
        {
          constantName: 'NUMBER_VALUE',
          default: 123,
          func: getEnvVariableNumber,
          description: 'my number',
        },
        {
          constantName: 'BOOLEAN_VALUE',
          default: true,
          func: getEnvVariableBoolean,
          description: 'my boolean',
        },
      ];

    it('should get the config settings with the default fallback values', () => {
      const settings = getConfigSettingsEnvTemplate(
        inboxSettingsMap,
        'MAIN_',
        'FALLBACK_',
      );
      const expected = `# | FALLBACK_STRING_VALUE | string | "test-string" | my string |
FALLBACK_STRING_VALUE=test-string
# | FALLBACK_NUMBER_VALUE | number | 123 | my number |
FALLBACK_NUMBER_VALUE=123
# | FALLBACK_BOOLEAN_VALUE | boolean | true | my boolean |
FALLBACK_BOOLEAN_VALUE=true
`;

      expect(settings).toBe(expected);
    });
  });
});
