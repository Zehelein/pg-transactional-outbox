import { sleep } from '../../common/utils';
import { TransactionalMessage } from '../../message/transactional-message';
import { ReplicationConcurrencyController } from './concurrency-controller';
import { createReplicationMultiConcurrencyController } from './create-multi-concurrency-controller';

const protectedAsyncFunction = async (
  controller: ReplicationConcurrencyController,
  task: (message: OrderMessage) => Promise<void>,
  message: TransactionalMessage,
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
  segment: string,
  aggregateType = 'task',
): TransactionalMessage => {
  return {
    aggregateId: id.toString(),
    aggregateType,
    messageType: 'order',
    segment,
    payload: { id },
  } as TransactionalMessage;
};

interface OrderMessage {
  id: number;
}

describe('createReplicationMultiConcurrencyController', () => {
  it('Executes tasks in parallel when the full concurrency is selected', async () => {
    // Arrange
    const controller = createReplicationMultiConcurrencyController(
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

  it('Executes tasks in sequential order within a context but the contexts in parallel when the segment mutex is selected', async () => {
    // Arrange
    const controller = createReplicationMultiConcurrencyController(
      () => 'segment-mutex',
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

  it('calls and finishes tasks in the correct order when the mutex is selected', async () => {
    // Arrange
    const controller = createReplicationMultiConcurrencyController(
      () => 'mutex',
    );
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

  it('calls and finishes tasks in the correct order when the semaphore is selected', async () => {
    // Arrange
    const controller = createReplicationMultiConcurrencyController(
      () => 'semaphore',
      {
        maxSemaphoreParallelism: 2,
      },
    );
    const order: number[] = [];
    const firstTask = async () => {
      await sleep(30);
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
      protectedAsyncFunction(
        controller,
        thirdTask,
        createOutboxMessage(1, 'A'),
      ),
    ]);

    // Assert - verify the order of execution
    expect(order).toEqual([2, 3, 1]);
    expect(new Date().getTime() - start).toBeGreaterThanOrEqual(30);
    expect(new Date().getTime() - start).toBeLessThan(40);
  });

  it('Executes tasks in correct order when different concurrency types are combined', async () => {
    // Arrange
    const controller = createReplicationMultiConcurrencyController(
      (message) => {
        switch (message.aggregateType) {
          case 'A':
            return 'segment-mutex';
          case 'B':
            return 'full-concurrency';
          case 'C':
            return 'mutex';
          case 'D':
            return 'semaphore';
          default:
            throw new Error('unreachable');
        }
      },
      {
        maxSemaphoreParallelism: 2,
      },
    );
    const orderA: number[] = [];
    const orderB: number[] = [];
    const orderC: number[] = [];
    const orderD: number[] = [];
    const createTask =
      (orderArray: number[], sleepTime: number) =>
      async (message: OrderMessage) => {
        await sleep(sleepTime);
        orderArray.push(message.id);
      };
    const start = new Date().getTime();

    // Act: these will execute in parallel, but the mutex should ensure that tasks for A and B are completed in order.
    await Promise.all([
      // segment tasks (1 then 2 and in parallel 3 then 4 --> takes ~40ms in total)
      protectedAsyncFunction(
        controller,
        createTask(orderA, 20),
        createOutboxMessage(1, 'D1', 'A'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderA, 20),
        createOutboxMessage(2, 'D1', 'A'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderA, 20),
        createOutboxMessage(3, 'D2', 'A'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderA, 20),
        createOutboxMessage(4, 'D2', 'A'),
      ),

      // concurrent tasks (5+6 in parallel --> takes ~40ms)
      protectedAsyncFunction(
        controller,
        createTask(orderB, 40),
        createOutboxMessage(5, 'x', 'B'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderB, 40),
        createOutboxMessage(6, 'x', 'B'),
      ),

      // mutex tasks (7 then 8 --> takes ~40ms)
      protectedAsyncFunction(
        controller,
        createTask(orderC, 20),
        createOutboxMessage(7, 'x', 'C'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderC, 20),
        createOutboxMessage(8, 'x', 'C'),
      ),
      // semaphore tasks (9 and 10 then 11 --> takes ~40ms)
      protectedAsyncFunction(
        controller,
        createTask(orderD, 20),
        createOutboxMessage(9, 'x', 'D'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderD, 30),
        createOutboxMessage(10, 'x', 'D'),
      ),
      protectedAsyncFunction(
        controller,
        createTask(orderD, 20),
        createOutboxMessage(11, 'x', 'D'),
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

    expect(orderD).toEqual([9, 10, 11]);

    expect(diff).toBeGreaterThanOrEqual(40);
    expect(diff).toBeLessThan(60);
  });

  it('Cancel works for all types', async () => {
    // Arrange
    const controller = createReplicationMultiConcurrencyController(
      (message) => {
        switch (message.segment) {
          case 'A':
            return 'segment-mutex';
          case 'B':
            return 'full-concurrency';
          case 'C':
            return 'mutex';
          case 'D':
            return 'semaphore';
          default:
            throw new Error('unreachable');
        }
      },
      {
        maxSemaphoreParallelism: 2,
      },
    );
    const success: number[] = [];
    const error: number[] = [];
    const createTask =
      (orderArray: number[], sleepTime: number) =>
      async (message: OrderMessage) => {
        await sleep(sleepTime);
        orderArray.push(message.id);
      };

    // Act

    // segment tasks (1 and only then 2)
    protectedAsyncFunction(
      controller,
      createTask(success, 5),
      createOutboxMessage(1, 'A', 'D1'),
    ).catch(() => error.push(1));
    protectedAsyncFunction(
      controller,
      createTask(success, 50),
      createOutboxMessage(2, 'A', 'D1'),
    ).catch(() => error.push(2));

    // concurrent tasks (3+4 in parallel)
    protectedAsyncFunction(
      controller,
      createTask(success, 5),
      createOutboxMessage(3, 'B'),
    ).catch(() => error.push(3));
    protectedAsyncFunction(
      controller,
      createTask(success, 50),
      createOutboxMessage(4, 'B'),
    ).catch(() => error.push(4));

    // mutex tasks (5 then 6)
    protectedAsyncFunction(
      controller,
      createTask(success, 5),
      createOutboxMessage(5, 'C'),
    ).catch(() => error.push(5));
    protectedAsyncFunction(
      controller,
      createTask(success, 50),
      createOutboxMessage(6, 'C'),
    ).catch(() => error.push(6));

    // semaphore tasks (9 and 10 in parallel then 11)
    protectedAsyncFunction(
      controller,
      createTask(success, 5),
      createOutboxMessage(7, 'D'),
    ).catch(() => error.push(7));
    protectedAsyncFunction(
      controller,
      createTask(success, 5),
      createOutboxMessage(8, 'D'),
    ).catch(() => error.push(8));
    protectedAsyncFunction(
      controller,
      createTask(success, 50),
      createOutboxMessage(9, 'D'),
    ).catch(() => error.push(9));

    await sleep(5);
    controller.cancel();

    // Assert
    await sleep(100);
    expect(success.sort()).toEqual([1, 3, 4, 5, 7, 8]);
    expect(error.sort()).toEqual([2, 6, 9]);
  });
});
