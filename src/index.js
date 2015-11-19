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

setOrawrapConfig()
  .then(orawrap => {
    oraWrapInst = orawrap;
    getPackage().then(jsonPackage => {
      program
        .version(jsonPackage.version)
        .usage('[options] <file ...>')
        .option('-v, --version', 'Print Version')
        .command('import <file> [otherFiles...]')
        .action((file, otherFiles) => {
          promptHandler(file);
        });

      program.parse(process.argv);
    });
  });
