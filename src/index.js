#!/usr/bin/env node

require('babel-polyfill');
import program from 'commander';
import fs from 'fs-promise';

import split from 'split';
import promptHandler from './prompt';
import {
  setOrawrapConfig
}
from './database';

import {
  Observable
}
from '@reactivex/rxjs';
import {
  createReadStream
}
from 'fs';

import {
  zipObject
}
from 'lodash';
import csv from 'csv';
import Bluebird from 'bluebird';

import {
  msExecute
}
from './database';

export var oraWrapInst;

var Promise = Bluebird;

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
  oraWrapInst = await setOrawrapConfig();
  let jsonPackage = await getPackage();
  program
    .version(jsonPackage.version)
    .usage('[command] [options] [file ...]')
    .option('-v, --version', 'Print Version')
    .option('-db, --database [value]', 'Use a database as the data source')
    .command('import [file]')
    .action((file) => {
      try {
        if (program.database && file) {
          throw new Error(`Expected either a database or file to be given, was given both: database == ${program.database}, file == ${file}`);
        } else if (file) {
          let csvObservable = createCsvObservable(file);
          promptHandler(csvObservable, file);
        } else if (program.database) {
          // TODO: create database observable(s) here...
        }
      } catch (e) {
        console.error(e.stack);
      }
    });

  program.parse(process.argv);
}

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
  }

  let column = source
    .flatMap(line => {
      return Observable.fromPromise(asyncCsvParse(line, csvOpts));
    })
    .take(1);

  return source
    .flatMap(line => {
      return Observable.fromPromise(asyncCsvParse(line, csvOpts));
    })
    .skip(1)
    .withLatestFrom(column, (row, header) => {
      return zipObject(header, row);
    });
}

main();
