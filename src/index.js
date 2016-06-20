#!/usr/bin/env node
Error.stackTraceLimit = Infinity;
require('babel-polyfill');

import program from 'commander';
import fs from 'fs-promise';
import split from 'split';
import winston from 'winston';
import { Observable } from '@reactivex/rxjs';
import { createReadStream } from 'fs';
import { zipObject } from 'lodash';
import csv from 'csv';
import Bluebird from 'bluebird';

import { msExecute } from './database';
import { promptHandlerFile, promptHandlerSams } from './prompt';
import { setOrawrapConfig } from './database';

export var oraWrapInst;
export var logger;
export var config;

var Promise = Bluebird;

/**
 * @return {object}
 */
async function getConfig() {
  try {
    let rawConfig = await fs.readFile('config.json');
    return JSON.parse(rawConfig);
  } catch(e) {
    console.error(e.stack);
  }
}

/**
 * @return {object}
 */
async function getPackage() {
  let rawPackage = await fs.readFile('package.json');
  return JSON.parse(rawPackage);
}

function asyncCsvParse(str, options) {
  return new Promise((resolve, reject) => {
    csv.parse(str, options, (err, output) => {
      if (err) {
        reject(err);
      } else if (output) {
        resolve(output[0]);
      }
    });
  });
}

async function main() {
  // Set Globals
  logger = new winston.Logger({
    transports: [
      new winston.transports.File({
        json: false,
        filename: `test-forge-${new Date()}.log`
      })
    ],
    exitOnError: false
  });
  logger.log('info', `starting up test-forge`);
  config = await getConfig();
  try {
    oraWrapInst = await setOrawrapConfig();
  } catch (e) {
    console.log('error setting up orawrap');
    console.log(e);
  }
  console.log('in main');

  let jsonPackage = await getPackage();
  program
    .version(jsonPackage.version)
    .usage('[command] [options] [file ...]')
    .option('-v, --version', 'Print Version')
    .option('-db, --database [value]', 'Use a database as the data source')
    .option('-t, --test [value]', 'Specify which test to create test data for (only used when using database as source)')
    .command('import [file]')
    .action(file => {

      try {
        if (program.database && file) {
          throw new Error(`Expected either a database or file to be given, was given both: database == ${program.database}, file == ${file}`);
        } else if (file) {
          promptHandlerFile(createCsvObservable(file), file);
        } else if (program.database && program.test && program.test === 'CRT') {
          console.log('in database prompt handler sams');
          promptHandlerSams(program.test);
        }
      } catch (e) {
        console.error(e.stack);
      }
    });
  program.parse(process.argv);
}

// TODO: only works on files with >1 lines. Make this work with source file with only 1 line.
function createCsvObservable(file) {
  let source = new Observable(o => {
    let fileStream = createReadStream(file)
      .pipe(
        split(null, null, {
          trailing: false
        })
      );
    fileStream.on('data', line => o.next(line));
    fileStream.on('error', err => o.error(err));
    fileStream.on('end', () => o.complete());
  });

  let csvOpts = {
    delimiter: '\t'
  };

  let csvParse = Observable.bindCallback(csv.parse);
  let column = source
    .flatMap(line => {
      return csvParse(line, csvOpts);
    })
    .take(1);

  return source
    .flatMap(line => {
      return csvParse(line, csvOpts);
    })
    .skip(1)
    .withLatestFrom(column, (row, header) => {
      const headerArray = header[1][0];
      const rowArray = row[1][0];
      return zipObject(headerArray, rowArray);
    });
}

main();
