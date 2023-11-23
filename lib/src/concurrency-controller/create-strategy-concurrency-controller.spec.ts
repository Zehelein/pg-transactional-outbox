import { OutboxMessage } from '../../dist';
import { sleep } from '../common/utils';
import { ConcurrencyController } from './concurrency-controller';
import { createStrategyConcurrencyController } from './create-strategy-concurrency-controller';

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
  aggregateType = 'task',
): OutboxMessage => {
  return {
    aggregateId: id.toString(),
    aggregateType,
    messageType,
    payload: { id },
  } as OutboxMessage;
};

interface OrderMessage {
  id: number;
}

describe('createStrategyConcurrencyController', () => {
  it('Executes tasks in parallel for the full concurrency strategy', async () => {
    // Arrange
    const controller = createStrategyConcurrencyController(
      () => 'full-concurrency',
    );
    const task = async () => {
      await sleep(50);
    };
    const start = new Date().getTime();

    // Act: these will execute in parallel and should not wait for each other
    await Promise.all([
      protectedAsyncFunction(controller, task, createOutboxMessage(1, 'A')),
      protectedAsyncFunction(controller, task, createOutboxMessage(2, 'B')),
    ]);

    // Assert - verify the order of execution
    const diff = new Date().getTime() - start;
    expect(diff).toBeGreaterThanOrEqual(50);
    expect(diff).toBeLessThan(100);
  });

  it('Executes tasks in sequential order within a context but the contexts in parallel for the discriminating mutex strategy', async () => {
    // Arrange
    const controller = createStrategyConcurrencyController(
      () => 'discriminating-mutex',
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

  it('calls and finishes tasks in the correct order for the mutex strategy', async () => {
    // Arrange
    const controller = createStrategyConcurrencyController(() => 'mutex');
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
      protectedAsyncFunction(
        controller,
        firstTask,
        createOutboxMessage(1, 'A'),
      ),
      protectedAsyncFunction(
        controller,
        secondTask,
        createOutboxMessage(1, 'A'),
      ),
    ]);

    // Assert - verify the order of execution
    expect(order).toEqual([1, 2]);
    expect(new Date().getTime() - start).toBeGreaterThanOrEqual(40);
  });

  it('Executes tasks in correct order when different concurrency strategies are combined', async () => {
    // Arrange
    const controller = createStrategyConcurrencyController(
      (message) => {
        switch (message.messageType) {
          case 'A':
            return 'discriminating-mutex';
          case 'B':
            return 'full-concurrency';
          default:
            return 'mutex';
        }
      },
      (message) => message.aggregateType,
    );
    const orderA: number[] = [];
    const orderB: number[] = [];
    const orderC: number[] = [];
    const createTask =
      (orderArray: number[], sleepTime: number) =>
      async (message: OrderMessage) => {
        await sleep(sleepTime);
        orderArray.push(message.id);
      };
    const start = new Date().getTime();

    // Act: these will execute in parallel, but the mutex should ensure that tasks for A and B are completed in order.
    await Promise.all([
      // discriminating tasks (1 then 2 and in parallel 3 then 4 --> takes ~40ms in total)
      protectedAsyncFunction(
        controller,
        createTask(orderA, 20),
        createOutboxMessage(1, 'A', 'D1'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderA, 20),
        createOutboxMessage(2, 'A', 'D1'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderA, 20),
        createOutboxMessage(3, 'A', 'D2'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderA, 20),
        createOutboxMessage(4, 'A', 'D2'),
      ),

      // concurrent tasks (5+6 in parallel --> takes ~40ms)
      protectedAsyncFunction(
        controller,
        createTask(orderB, 40),
        createOutboxMessage(5, 'B'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderB, 40),
        createOutboxMessage(6, 'B'),
      ),

      // mutex tasks (7 then 8 --> takes ~40ms)
      protectedAsyncFunction(
        controller,
        createTask(orderC, 20),
        createOutboxMessage(7, 'C'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderC, 20),
        createOutboxMessage(8, 'C'),
      ),
    ]);

    // Assert - verify that different strategies run in parallel and within in the desired concurrency logic
    const diff = new Date().getTime() - start;
    expect(orderA).toHaveLength(4);
    expect(orderA.indexOf(1)).toBeLessThan(orderA.indexOf(2));
    expect(orderA.indexOf(3)).toBeLessThan(orderA.indexOf(4));

    expect(orderB).toHaveLength(2);
    expect(orderB).toContain(5);
    expect(orderB).toContain(6);

    expect(orderC).toEqual([7, 8]);

    expect(diff).toBeGreaterThanOrEqual(40);
    expect(diff).toBeLessThan(60);
  });
});
