import inspector from 'inspector';

/** Sleep for a given amount of milliseconds */
export const sleep = async (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

/** Sleep until the callback evaluates to true. Return the elapsed amount of milliseconds */
export const sleepUntilTrue = async <T>(
  callback: () => boolean | Promise<boolean>,
  timeout: number,
  delay = 1,
): Promise<number> => {
  const start = Date.now();
  while (!(await callback())) {
    // If the timeout is exceeded throw
    if (Date.now() - start > timeout) {
      throw new Error(`Sleep timeout of ${timeout}ms reached`);
    }
    // recheck again after a delay
    await sleep(delay);
  }
  return Date.now() - start;
};

/** Check if the tests are run in debug mode right now */
export const isDebugMode = (): boolean => inspector.url() !== undefined;

/** Try to execute some code in a loop until it either succeeds, a jest error was raised, or the timeout is reached */
export const retryCallback = async <T>(
  callback: () => Promise<T>,
  timeout: number,
  delay: number,
): Promise<T> => {
  const start = Date.now();
  // eslint-disable-next-line no-constant-condition
  while (true) {
    try {
      return await callback();
    } catch (error) {
      // Check if it is a jest error and throw if it is
      if (
        typeof error === 'object' &&
        error !== null &&
        'matcherResult' in error &&
        error.matcherResult
      ) {
        throw error;
      }
      // If the timeout is exceeded throw
      if (Date.now() - start > timeout) {
        throw new Error(`Retry timeout of ${timeout}ms reached`);
      }
      // retry again after a delay
      await sleep(delay);
    }
  }
};
