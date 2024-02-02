import { sleep } from '../common/utils';
import { ConcurrencyController } from '../concurrency-controller/concurrency-controller';
import { TransactionalMessage } from '../message/message';
import { defaultConcurrencyStrategy } from './concurrency-strategy';

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

describe('defaultConcurrencyStrategy', () => {
  it('uses the mutex strategy to call and finish tasks in the correct order', async () => {
    // Arrange
    const controller = defaultConcurrencyStrategy();
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
});
