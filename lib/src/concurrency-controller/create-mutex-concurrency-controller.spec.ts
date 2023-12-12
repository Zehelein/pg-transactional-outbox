import { OutboxMessage } from '../common/message';
import { sleep } from '../common/utils';
import { ConcurrencyController } from './concurrency-controller';
import { createMutexConcurrencyController } from './create-mutex-concurrency-controller';

const protectedAsyncFunction = async (
  controller: ConcurrencyController,
  body: () => Promise<void>,
) => {
  const release = await controller.acquire({} as OutboxMessage);
  try {
    await body();
  } finally {
    release();
  }
};

describe('createMutexConcurrencyController', () => {
  it('calls and finishes tasks in the correct order', async () => {
    // Arrange
    const controller = createMutexConcurrencyController();
    const order: number[] = [];
    const firstTask = async () => {
      await sleep(30);
      order.push(1);
    };
    const secondTask = async () => {
      await sleep(10);
      order.push(2);
    };
    const start = new Date().getTime();

    // Act - these will execute in parallel, but the mutex should ensure they complete in order.
    await Promise.all([
      protectedAsyncFunction(controller, firstTask),
      protectedAsyncFunction(controller, secondTask),
    ]);

    // Assert - verify the order of execution
    expect(order).toEqual([1, 2]);
    expect(new Date().getTime() - start).toBeGreaterThanOrEqual(40);
  });

  it('Cancels the mutex', async () => {
    // Arrange
    const controller = createMutexConcurrencyController();
    const order: number[] = [];
    const error: number[] = [];
    const successTask = (orderArray: number[]) => async () => {
      await sleep(20);
      orderArray.push(1);
    };
    const errorTask = (orderArray: number[]) => async () => {
      await sleep(20);
      orderArray.push(2);
    };

    // Act

    // This tasks will start immediately as it does not have to wait for a mutex
    protectedAsyncFunction(controller, successTask(order)).catch(() =>
      error.push(1),
    );

    // This task will have to wait for the mutex to be free but the cancel further down will be faster
    protectedAsyncFunction(controller, errorTask(order)).catch(() =>
      error.push(2),
    );

    controller.cancel();

    // Assert
    await sleep(200);
    expect(order).toEqual([1]);
    expect(error).toEqual([2]);
  });
});
