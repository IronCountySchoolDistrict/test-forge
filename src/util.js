import util from 'util';

/**
 * use util.inspect() to print an object
 * @param  {object} obj
 * @return {string}     printed object
 */
export function printObj(obj) {
  return util.inspect(obj, {
    showHidden: false,
    depth: null
  });
}
