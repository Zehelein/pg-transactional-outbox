import { sleep } from '../../common/utils';
import { TransactionalMessage } from '../../message/transactional-message';
import { ReplicationConcurrencyController } from '../concurrency-controller/concurrency-controller';
import { defaultReplicationConcurrencyStrategy } from './concurrency-strategy';

const protectedAsyncFunction = async (
  controller: ReplicationConcurrencyController,
  body: () => Promise<void>,
) => {
  const release = await controller.acquire({} as TransactionalMessage);
  try {
    await body();
  } finally {
    release();
  }
};

describe('defaultReplicationConcurrencyStrategy', () => {
  it('uses the mutex strategy to call and finish tasks in the correct order', async () => {
    // Arrange
    const controller = defaultReplicationConcurrencyStrategy();
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
