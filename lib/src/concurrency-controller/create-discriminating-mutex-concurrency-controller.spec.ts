import { OutboxMessage } from '../common/message';
import { sleep } from '../common/utils';
import { ConcurrencyController } from './concurrency-controller';
import { createDiscriminatingMutexConcurrencyController } from './create-discriminating-mutex-concurrency-controller';

const protectedAsyncFunction = async (
  controller: ConcurrencyController,
  task: (message: OrderMessage) => Promise<void>,
  message: OutboxMessage,
) => {
  const release = await controller.acquire(message);
  try {
    await task(message.payload as OrderMessage);
  } finally {
    release();
  }
};

const createOutboxMessage = (
  id: number,
  messageType: string,
): OutboxMessage => {
  return {
    aggregateId: id.toString(),
    aggregateType: 'task',
    payload: { id },
    messageType,
  } as OutboxMessage;
};

interface OrderMessage {
  id: number;
}

describe('createDiscriminatingMutexConcurrencyController', () => {
  it('Executes tasks in sequential order within a context but the contexts in parallel', async () => {
    // Arrange
    const controller = createDiscriminatingMutexConcurrencyController(
      (message) => message.messageType,
    );
    const orderA: number[] = [];
    const orderB: number[] = [];
    const firstTask =
      (orderArray: number[]) => async (message: OrderMessage) => {
        await sleep(30);
        orderArray.push(message.id);
      };
    const secondTask =
      (orderArray: number[]) => async (message: OrderMessage) => {
        await sleep(10);
        orderArray.push(message.id);
      };
    const start = new Date().getTime();

    // Act: these will execute in parallel, but the mutex should ensure that tasks for A and B are completed in order.
    await Promise.all([
      protectedAsyncFunction(
        controller,
        firstTask(orderA),
        createOutboxMessage(1, 'A'),
      ),
      protectedAsyncFunction(
        controller,
        secondTask(orderA),
        createOutboxMessage(2, 'A'),
      ),
      protectedAsyncFunction(
        controller,
        firstTask(orderB),
        createOutboxMessage(1, 'B'),
      ),
      protectedAsyncFunction(
        controller,
        secondTask(orderB),
        createOutboxMessage(2, 'B'),
      ),
    ]);

    // Assert - verify the order of execution
    const diff = new Date().getTime() - start;
    expect(orderA).toEqual([1, 2]);
    expect(orderB).toEqual([1, 2]);
    expect(diff).toBeGreaterThanOrEqual(40);
    expect(diff).toBeLessThan(80);
  });

  it('Cancels all mutexes', async () => {
    // Arrange
    const controller = createDiscriminatingMutexConcurrencyController(
      (message) => message.messageType,
    );
    const orderA: number[] = [];
    const errorA: number[] = [];
    const orderB: number[] = [];
    const errorB: number[] = [];
    const successTask = (orderArray: number[]) => async () => {
      await sleep(20);
      orderArray.push(1);
    };
    const errorTask = (orderArray: number[]) => async () => {
      await sleep(20);
      orderArray.push(2);
    };

    // Act: these will execute in parallel - first tasks should succeed but second tasks wait and get cancelled while waiting
    protectedAsyncFunction(
      controller,
      successTask(orderA),
      createOutboxMessage(1, 'A'),
    ).catch(() => errorA.push(1));
    protectedAsyncFunction(
      controller,
      errorTask(orderA),
      createOutboxMessage(2, 'A'),
    ).catch(() => errorA.push(2));
    protectedAsyncFunction(
      controller,
      successTask(orderB),
      createOutboxMessage(1, 'B'),
    ).catch(() => errorB.push(1));
    protectedAsyncFunction(
      controller,
      errorTask(orderB),
      createOutboxMessage(2, 'B'),
    ).catch(() => errorB.push(2));

    // This cancels all controllers (including controllerB)
    controller.cancel();

    // Assert - verify the order of execution
    await sleep(200);
    expect(orderA).toEqual([1]);
    expect(orderB).toEqual([1]);
    expect(errorA).toEqual([2]);
    expect(errorB).toEqual([2]);
  });
});
