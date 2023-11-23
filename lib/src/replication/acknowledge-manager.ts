import { TransactionalLogger } from '../common/logger';

export interface AcknowledgeManager {
  startProcessingLSN: (lsn: string) => void;
  finishProcessingLSN: (lsn: string) => void;
}

/**
 * This LSN acknowledge manager cares about remembering the LSN numbers that
 * were sent from PostgreSQL and acknowledge the LSN only after all the LSNs
 * before were acknowledged as well. With the `startProcessingLSN` it tracks
 * the LSN and with `finishProcessingLSN` it marks this LSN as finished. It
 * checks then which (if any) LSN can be acknowledged (no older LSNs are in
 * pending state) and executed the `acknowledgeLsn` callback.
 * @param acknowledgeLsn Callback to actually acknowledge the WAL message
 * @param logger A logger instance for logging trace up to error logs
 * @returns two functions - one to be called when starting the WAL processing and one when the processing is done and the acknowledgement can be done.
 */
export const createAcknowledgeManager = (
  acknowledgeLsn: (lsn: string) => void,
  logger: TransactionalLogger,
): AcknowledgeManager => {
  const processingMap = new Map<string, bigint>();
  const pendingAckMap = new Map<string, bigint>();

  const lsnToBigInt = (lsn: string): bigint => {
    return BigInt('0x' + lsn.replace('/', ''));
  };

  const checkForAcknowledgeableLSN = (
    currentLsn: string,
    currentLsnNumber: bigint,
  ): void => {
    const sortedProcessingLsns = Array.from(processingMap.entries()).sort(
      (a, b) => Number(a[1] - b[1]),
    );
    processingMap.delete(currentLsn);
    pendingAckMap.set(currentLsn, currentLsnNumber);

    if (sortedProcessingLsns[0][0] !== currentLsn) {
      // there is still some message processed with a lower LSN
      return;
    }

    const [_nextLsn, nextLsnNumber] = sortedProcessingLsns[1] ?? [
      '0/ffffffffffffffff',
      BigInt('18446744073709551615'), // 64bit unsigned int is the maximum Postgres WAL LSN value
    ];
    const sortedPendingAckLsns = Array.from(pendingAckMap.entries()).sort(
      (a, b) => Number(a[1] - b[1]),
    );

    let ackLsn: string | undefined = undefined;
    for (const [lsn, lsnNumber] of sortedPendingAckLsns) {
      if (lsnNumber < nextLsnNumber) {
        ackLsn = lsn;
        pendingAckMap.delete(lsn);
      } else {
        break;
      }
    }

    if (ackLsn) {
      logger.trace(`Acknowledging LSN up to ${ackLsn}`);
      acknowledgeLsn(ackLsn);
    }
  };

  const startProcessingLSN = (lsn: string): void => {
    if (processingMap.has(lsn)) {
      throw new Error(`LSN ${lsn} is already being processed.`);
    }
    processingMap.set(lsn, lsnToBigInt(lsn));
  };

  const finishProcessingLSN = (lsn: string): void => {
    const lsnNumber = processingMap.get(lsn);
    if (lsnNumber === undefined) {
      throw new Error(`LSN ${lsn} was not registered as processing.`);
    }
    logger.trace(`Finished LSN ${lsn} - waiting for acknowledgement.`);
    checkForAcknowledgeableLSN(lsn, lsnNumber);
  };

  return { startProcessingLSN, finishProcessingLSN };
};
