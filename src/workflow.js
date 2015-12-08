require('babel-polyfill');
import { gustav } from 'gustav';
import { mapValues } from 'lodash';
import Dibels from './testClasses';
import json2csv from 'json2csv';
import Bluebird from 'bluebird';
import fs from 'fs-promise';
import { basename, extname } from 'path';
import { EOL } from 'os';
import prependFile from 'prepend-file';
import detect from './detector';
import { getMatchingStudentTest } from './service';

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
    .transf(transform)
    .sink(csvNode, {
      file: file,
      promptOpts: promptOpts
    });

  workflow.start();
}

function transform(observer) {
  return observer.flatMap(async function(item) {
    console.dir(item);
    let testObj;
    if (detect(item) === 'dibels') {
      testObj = new Dibels(item, promptOpts.testId);
    }

    if (promptOpts.table === 'Test Results') {
      console.log('in test results');
      let studentTests = await getMatchingStudentTest(item['Student Primary ID'], item['School Year'], promptOpts.testId)
      if (!studentTests.rows.length) {
        console.log('returning test results csv');
        return await testObj.toTestResultsCsv();
      }

    } else if (promptOpts.table === 'U_StudentTestProficiency') {
      console.log('returning proficiency');
      return await testObj.toProficiencyCsv();
    } else if (promptOpts.table === 'U_StudentTestSubscore') {
      console.log('returning subscore');
      return await testObj.toTestResultsCsv();
    } else {
      console.log('returning nothing');
      return 'nothing';
    }
  });
}

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

  return observer.skip(2).subscribe(async function(item) {
    let csvStr = await toCSV({
      data: item,
      del: '\t',
      hasCSVColumnTitle: false
    });
    csvStr = csvStr.replace(/"/g, '');

    await fs.appendFile(`output/${outputFilename}`, `${EOL}${csvStr}`);
  })
}
