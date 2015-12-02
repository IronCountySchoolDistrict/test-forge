#!/usr/bin/env node
require('babel-polyfill');
import program from 'commander';
import fs from 'fs-promise';

import promptHandler from './prompt';
import { setOrawrapConfig } from './database';

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
      promptHandler(file);
    });

  program.parse(process.argv);
}

main();
