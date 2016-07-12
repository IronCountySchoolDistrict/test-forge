require('babel-polyfill');

import Promise from 'bluebird';
import orawrap from 'orawrap';
import oracledb from 'oracledb';
import fs from 'fs-promise';
import { Observable } from '@reactivex/rxjs';
import mssql from 'mssql';

import { oraWrapInst, config } from './index';

var msConnPool;
var oraConnPool;


/**
 * orawrap requires only the parameters that will be used be passed to it. This function removes the null args,
 * and creates a Promise around the orawrap execute function.
 * @param  {string} sql SQL string
 * @param  {object} bind Oracle bind variables
 * @param  {object} [opts]
 * @return {Promise}
 */
export function execute(sql, bind, opts) {
  if (!oraConnPool) {
    createOraPool();
  }
  return oraConnPool
    .then(pool => pool.getConnection())
    .then(conn => conn.execute(sql, bind, opts));
}

function createOraPool() {
  let oraPool = oracledb.createPool(config.database.oracle);
  oraConnPool = oraPool;
  return oraPool;
}

/**
 *
 * @return {Promise}
 */
function createMsConn() {
  config.database.sams.requestTimeout = 600000;
  let msConn = mssql.connect(config.database.sams);
  msConnPool = msConn;
  return msConn;
}

export function closeMsConn() {
  if (msConnPool) {
    msConnPool.then((connection) => {
      connection.close();
    });
  }
}

/**
 * create and execute a SQL query
 * @param {string} sql
 * @param {object} inputParams
 * @return {observable}
 */
export function msExecute(sql, inputParams) {
  return new Observable(observer => {
    if (!msConnPool) {
      createMsConn();
    }
    msConnPool
      .then((connection) => {
        var request = new mssql.Request(msConnPool);
        if (inputParams) {
          Object.keys(inputParams).forEach(paramName => {
            request.input(paramName, inputParams[paramName]);
          });
        }
        request.stream = true;
        request.query(sql);
        request.on('row', row => {
          observer.next(row);
        });
        request.on('error', err => {
          console.log('error == ', err);
          observer.error(err);
        });
        request.on('done', (returnValue, affected) => {
          observer.complete();
        });
      })
      .catch(error => console.log(`mssql error == ${error}`));
  });
}
