require('babel-polyfill');

import Promise from 'bluebird';
import orawrap from 'orawrap';

import readFile from 'fs-readfile-promise';

import { oraWrapInst } from './index';

export async function setOrawrapConfig() {
  let oraWrapInst = orawrap;
  let config = await readFile('./config.json');
  oraWrapInst.setConnectInfo(JSON.parse(config.toString()));
  return oraWrapInst;
}

/**
 * orawrap requires only the parameters that will be used be passed to it. This function removes the null args,
 * and creates a Promise around the orawrap execute function.
 * @param  {string} sql SQL string
 * @param  {object} bind Oracle bind variables
 * @param  {object} [opts]
 * @return {Promise}      resolves if no errors were returned from orawrap.execute, rejects with errors if there were any
 */
export function execute(sql, bind, opts) {
  let args = [];
  for (let i = 0; i < arguments.length; i++) {
    args.push(arguments[i]);
  }

  // remove any null arguments
  args.filter(elem => !!elem);

  return new Promise(function(resolve, reject) {
    let cb = function(err, results) {
      if (err) {
        reject(err);
      }
      resolve(results);
    }
    args.push(cb);

    //global orawrap instance created in index.js
    try {
      oraWrapInst.execute.apply(orawrap, args);
    } catch (e) {
      console.error(e.stack);
    }
  });
}
