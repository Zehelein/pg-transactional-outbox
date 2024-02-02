import { sleep } from '../common/utils';
import { TransactionalMessage } from '../message/message';
import { ConcurrencyController } from './concurrency-controller';
import { createFullConcurrencyController } from './create-full-concurrency-controller';

const protectedAsyncFunction = async (
  controller: ConcurrencyController,
  body: () => Promise<void>,
) => {
  const release = await controller.acquire({} as TransactionalMessage);
  try {
    await body();
  } finally {
    release();
  }
};

describe('createFullConcurrencyController', () => {
  it('Executes tasks in parallel', async () => {
    // Arrange
    const controller = createFullConcurrencyController();
    const task = async () => {
      await sleep(50);
    };
    const start = new Date().getTime();

    // Act: these will execute in parallel and should not wait for each other
    await Promise.all([
      protectedAsyncFunction(controller, task),
      protectedAsyncFunction(controller, task),
    ]);

    // Assert - verify the order of execution
    const diff = new Date().getTime() - start;
    expect(diff).toBeGreaterThanOrEqual(50);
    expect(diff).toBeLessThan(100);
  });

  it('Cancel has no effect', async () => {
    // Arrange
    const controller = createFullConcurrencyController();
    const items: number[] = [];
    const task = (id: number) => async () => {
      await sleep(50);
      items.push(id);
    };

    // Act: these will execute in parallel and should not wait for each other
    void protectedAsyncFunction(controller, task(1));
    void protectedAsyncFunction(controller, task(2));

    controller.cancel();

    // Assert - verify the order of execution
    await sleep(100);
    expect(items.sort()).toEqual([1, 2]);
  });
});
