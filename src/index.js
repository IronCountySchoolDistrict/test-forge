require('babel-polyfill');
import program from 'commander';
import readFile from 'fs-readfile-promise';

import promptHandler from './prompt';
import { setOrawrapConfig } from './database';

export var oraWrapInst;

async function getPackage() {
  let rawPackage = await readFile('package.json');
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
        .action(function(file, otherFiles) {
          promptHandler(file);
        });

      program.parse(process.argv);
    });
  });
