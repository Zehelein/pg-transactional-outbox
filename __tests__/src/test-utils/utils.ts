export const sleep = async (milliseconds: number): Promise<unknown> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

export const tryCallback = async <T>(
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
      if (Date.now() - start > timeout) {
        throw new Error(`Timeout of ${timeout}ms reached`);
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }
};
