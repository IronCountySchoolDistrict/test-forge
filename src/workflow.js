require('babel-polyfill');
import { gustav } from 'gustav';
import { mapValues } from 'lodash';
import Dibels from './dibels';
import { CRTTestResults } from './crt';
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

/**
 * [workflow description]
 * @param  {[type]} sourceObservable [description]
 * @param  {[type]} promptOpts       [description]
 * @param  {[type]} file             [description]
 * @param  {string} test             test options passed in through command-line
 * @return {[type]}                  [description]
 */
export default function workflow(sourceObservable, promptOpts, file, test) {
  gustav.source('dataSource', () => sourceObservable);

  let workflow = gustav.createWorkflow()
    .source('dataSource')
    .transf(transform, {
      promptOpts: promptOpts,
      test: test
    })
    .transf(filterEmpty)
    .sink(consoleNode);
    // .sink(csvNode, {
    //   file: file,
    //   promptOpts: promptOpts
    // });

  workflow.start();
}

function transform(config, observer) {
  let testObj;
  return observer
    .flatMap(item => {
      console.log(item);
      // test was passed in through command-line prompt response
      // detect test by the type of data passed in
      if (config.promptOpts.test) {
        let test = detect(item);
        if (test.type === 'dibels') {
          testObj = new Dibels(item, config.promptOpts);
        }

      // test was passed in through command-line option
      } else {
        if (config.test === 'CRT') {
          if (config.promptOpts.table === 'Test Results') {
            testObj = new CRTTestResults(item, config.promptOpts);
          }
        }
      }
      return testObj.createTransformer(config.promptOpts, item);
    });
}

function filterEmpty(observer) {
  return observer.filter(item => {
    return !isEmpty(item);
  });
}

let consoleNode = iO => iO.subscribe(x=>{
  console.dir(x);
});

function csvNode(config, observer) {
  let outputFilename;
  if (config.file) {
    outputFilename = `${basename(config.file, extname(config.file))}-${config.promptOpts.table}${extname(config.file)}`;
  } else {
    outputFilename = `${config.promptOpts.table}${extname(config.file)}`;
  }
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
