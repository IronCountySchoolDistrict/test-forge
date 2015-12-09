require('babel-polyfill');
import { gustav } from 'gustav';
import { mapValues } from 'lodash';
import Dibels from './dibels';
import json2csv from 'json2csv';
import Bluebird from 'bluebird';
import fs from 'fs-promise';
import { basename, extname } from 'path';
import { EOL } from 'os';
import prependFile from 'prepend-file';
import detect from './detector';
import { getMatchingStudentTest } from './service';
import { isEmpty } from 'lodash';
import { Observable } from '@reactivex/rxjs';

var Promise = Bluebird;
var toCSV = Bluebird.promisify(json2csv);

function asyncPrependFile(file, content) {
  return new Promise((resolve, reject) => {
    prependFile(file, content, err => {
      if (err) {
        reject(err);
      }
      resolve();
    });
  })
}

export default function workflow(sourceObservable, promptOpts, file) {
  gustav.source('csvSource', () => sourceObservable);

  let workflow = gustav.createWorkflow()
    .source('csvSource')
    .transf(transform, promptOpts)
    .transf(filterEmpty)
    .sink(csvNode, {
      file: file,
      promptOpts: promptOpts
    });

  workflow.start();
}

function transform(promptOpts, observer) {
  let testObj;
  return observer
    .flatMap(item => {
      let test = detect(item);
      if (test.type === 'dibels') {
        testObj = new Dibels(item, promptOpts);
      } else if (test.type === 'CRT') {
        testObj = new CRT(item, promptOpts);
      }
      return testObj.createTransformer(promptOpts, item);
    });
}

function filterEmpty(observer) {
  return observer.filter(item => {
    return !isEmpty(item);
  });
}

let consoleNode = iO => iO.subscribe(console.log);

function csvNode(config, observer) {
  let outputFilename = `${basename(config.file, extname(config.file))}-${config.promptOpts.table}${extname(config.file)}`;
  observer.first().subscribe(async function(item) {
    let csvStr = await toCSV({
      data: item,
      del: '\t',
      hasCSVColumnTitle: true
    });
    csvStr = csvStr.replace(/"/g, '');
    try {
      await fs.truncate(`output/${outputFilename}`);

      // input file name-import table name.file extension
      await asyncPrependFile(`output/${outputFilename}`, `${csvStr}`);
    } catch (e) {
      // input file name-import table name.file extension
      await asyncPrependFile(`output/${outputFilename}`, `${csvStr}`);
    }
  });

  return observer.skip(1).subscribe(async function(item) {
    let csvStr = await toCSV({
      data: item,
      del: '\t',
      hasCSVColumnTitle: false
    });
    csvStr = csvStr.replace(/"/g, '');

    await fs.appendFile(`output/${outputFilename}`, `${EOL}${csvStr}`);
  });
}
