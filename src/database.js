require('babel-polyfill');

import Promise from 'bluebird';
import orawrap from 'orawrap';
import fs from 'fs-promise';
import {
  Observable
} from '@reactivex/rxjs';
import mssql from 'mssql';

import {
  oraWrapInst,
  config
} from './index';

var msConnPool;

export async function setOrawrapConfig() {
  let oraWrapInst = orawrap;
  return new Promise((resolve, reject) => {
    oraWrapInst.createPool(config.database.oracle, (err, pool) => {
      if (err) {
        reject(err);
      }
      resolve(oraWrapInst);
    });
  });
}

/**
 * orawrap requires only the parameters that will be used be passed to it. This function removes the null args,
 * and creates a Promise around the orawrap execute function.
 * @param  {string} sql SQL string
 * @param  {object} bind Oracle bind variables
 * @param  {object} [opts]
 * @return {Promise}
 */
export function execute(sql, bind, opts) {
  let args = [];
  for (let i = 0; i < arguments.length; i++) {
    args.push(arguments[i]);
  }

  // remove any null arguments
  args.filter(elem => !!elem);

  return new Promise((resolve, reject) => {
    let cb = function(err, results) {
      if (err) {
        reject(err);
      }
      resolve(results);
    };
    args.push(cb);
    //global orawrap instance created in index.js
    try {
      oraWrapInst.execute.apply(orawrap, args);
    } catch (e) {
      console.error(e.stack);
    }
  });
}

/**
 * create and execute an SQL query
 * @param {string} sql
 * @param {object} inputParams
 * @return {observable}
 */
export function msExecute(sql, inputParams) {
  return new Observable(observer => {
    config.database.sams.requestTimeout = 600000;

    if (!msConnPool) {
      var connection = new mssql.Connection(config.database.sams, err => {
        if (err) {
          console.log('error == ', err);
        }
        var request = new mssql.Request(connection);
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
          console.log('finished request');
          connection.close();
          observer.complete();
        });
      });
      connection.on('error', error => console.log(`mssql error == ${error}`));
      msConnPool = connection;
    }
  });
}
