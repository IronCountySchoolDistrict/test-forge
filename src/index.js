#!/usr/bin/env node

require('babel-polyfill');
import program from 'commander';
import fs from 'fs-promise';

import split from 'split';
import promptHandler from './prompt';
import { setOrawrapConfig } from './database';

import {   Observable } from '@reactivex/rxjs';
import { createReadStream } from 'fs';

import { zipObject } from 'lodash';
import csv from 'csv';
import Bluebird from 'bluebird';

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
  let orawrap = await setOrawrapConfig();
  oraWrapInst = orawrap;
  let jsonPackage = await getPackage();
  program
    .version(jsonPackage.version)
    .usage('[options] <file ...>')
    .option('-v, --version', 'Print Version')
    .command('import <file> [otherFiles...]')
    .action((file, otherFiles) => {
      try {
        let csvObservable = createCsvObservable(file);

        promptHandler(csvObservable);
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
      )
    fileStream.on('data', line => {
      o.next(line)
    });
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
