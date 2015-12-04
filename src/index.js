#!/usr/bin/env node
require('babel-polyfill');
import program from 'commander';
import fs from 'fs-promise';

import split from 'split';
import promptHandler from './prompt';
import { setOrawrapConfig } from './database';

import { Observable } from '@reactivex/rxjs';
import { createReadStream } from 'fs';

export var oraWrapInst;

async function getPackage() {
  let rawPackage = await fs.readFile('package.json');
  return JSON.parse(rawPackage);
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
        let source = new Observable(o => {
          let fileStream = createReadStream(file)
            .pipe(split());
          fileStream.on('data', line => o.next(line));
          fileStream.on('error', err => o.error(err););
          fileStream.on('end', () => o.complete());
        });

        promptHandler(source);
      } catch (e) {
        console.error(e.stack);
      }

    });

  program.parse(process.argv);
}

main();
