import { logger } from './index';
import { printObj } from './util';

export function logErrors(item, msg, e) {
  logger.log('info', msg, {
    psDbError: printObj(e)
  });
  if (item) {
    logger.log('info', 'Source Data Record: ', {
      sourceData: printObj(item)
    });
  }
}
