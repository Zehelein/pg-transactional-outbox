import { sleep } from '../common/utils';
import { TransactionalMessage } from '../message/message';
import { ConcurrencyController } from './concurrency-controller';
import { createSemaphoreConcurrencyController } from './create-semaphore-concurrency-controller';

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

describe('createSemaphoreConcurrencyController', () => {
  it('calls and finishes tasks in the correct order', async () => {
    // Arrange
    const controller = createSemaphoreConcurrencyController(2);
    const order: number[] = [];
    const firstTask = async () => {
      await sleep(50);
      order.push(1);
    };
    const secondTask = async () => {
      await sleep(10);
      order.push(2);
    };
    const thirdTask = async () => {
      await sleep(10);
      order.push(3);
    };
    const start = new Date().getTime();

    // Act - these will execute in parallel, but the semaphore should ensure they complete in order.
    await Promise.all([
      protectedAsyncFunction(controller, firstTask),
      protectedAsyncFunction(controller, secondTask),
      protectedAsyncFunction(controller, thirdTask),
    ]);

    // Assert - verify the order of execution
    expect(order).toEqual([2, 3, 1]);
    expect(new Date().getTime() - start).toBeGreaterThanOrEqual(40);
  });

  it('Cancels the semaphore', async () => {
    // Arrange
    const controller = createSemaphoreConcurrencyController(2);
    const order: number[] = [];
    const error: number[] = [];
    const successTask1 = (orderArray: number[]) => async () => {
      await sleep(20);
      orderArray.push(1);
    };
    const successTask2 = (orderArray: number[]) => async () => {
      await sleep(20);
      orderArray.push(2);
    };
    const errorTask = (orderArray: number[]) => async () => {
      await sleep(20);
      orderArray.push(3);
    };

    // Act

    // These two tasks will start immediately as it does not have to wait for a semaphore
    protectedAsyncFunction(controller, successTask1(order)).catch(() =>
      error.push(1),
    );
    protectedAsyncFunction(controller, successTask2(order)).catch(() =>
      error.push(2),
    );

    // This task will have to wait for the semaphore to be free but the cancel further down will be faster
    protectedAsyncFunction(controller, errorTask(order)).catch(() =>
      error.push(3),
    );

    controller.cancel();

    // Assert
    await sleep(200);
    expect(order.sort()).toEqual([1, 2]);
    expect(error).toEqual([3]);
  });
});
