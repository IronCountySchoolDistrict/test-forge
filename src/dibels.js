require('babel-polyfill');

import { createWriteStream } from 'fs';
import fs from 'fs-promise';
import { Observable } from '@reactivex/rxjs';
import Promise from 'bluebird';
import { gustav } from 'gustav';
import { basename, extname } from 'path';
import json2csv from 'json2csv';
import { EOL } from 'os';
import util from 'util';
import { isEmpty, merge } from 'lodash';

import {
  studentTestScoreDuplicateCheck,
  proficiencyDuplicateCheck,
  studentNumberToStudentId,
  testRecordToMatchingDcid
}
from './blogic';
import { printObj } from './util';
import { logger, config } from './index';

var toCSV = Promise.promisify(json2csv);

export function createWorkflow(sourceObservable, prompt, inputFile) {
  gustav.source('dibelsSource', () => sourceObservable);

  let config = {
    prompt: prompt,
    inputFile: inputFile
  };
  return gustav.createWorkflow()
    .source('dibelsSource')
    .transf(createTransformer, config)
    .sink(csvObservable, config);
}

function testResultsTransform(config, observable) {
  return observable
    .flatMap(item => {
      const matchingTestScore = studentTestScoreDuplicateCheck(
        item['Student Primary ID'],
        item['School Year'],
        item['Composite Score'],
        config.prompt.testId,
        'Composite',
        item);

      return Observable.zip(
        studentNumberToStudentId(item['Student Primary ID'], item).do(x => console.log('x == ', x)),
        matchingTestScore,

        (studentId, matchingTest) => {
          console.log('studentId == ', studentId);
          console.log('matchingTest == ', matchingTest);
          return {
            studentId: studentId,
            matchingTestScore: matchingTest
          };
        }
      );
    })
    .map(item => {
      console.log('item == ', item);
      return {
        'Test Date': item.config.testConstants.ROGL_Begin_Year.testDate,
        'Student Id': item.studentId,
        'Student Number': item.testResult['Student Primary ID'],
        'Grade Level': item.testResult.Grade,
        'Composite Score Alpha': item.testResult['Composite Score']
      };
    });
}

function proficiencyTransform(config, observable) {
  return observable
    .flatMap(item => {
      const matchingTestScore = testRecordToMatchingDcid(
        item['Student Primary ID'],
        item['School Year'],
        item['Composite Score'],
        config.prompt.testId,
        item);

      return Observable.zip(
        matchingTestScore,
        Observable.of(item),

        (s1, s2) => ({
          matchingTestScore: s1,
          record: s2
        })
      );
    })
    .flatMap(item => {
      let matchingProficiency = proficiencyDuplicateCheck(
        item.record['Student Primary ID'],
        item.record['School Year'],
        item.record['Composite Score'],
        config.prompt.testId,
        item
      );

      return Observable.zip(
        matchingProficiency,

        Observable.of(item),

        (_, testRecord) => ({
          testResult: testRecord.record,
          studentTestScore: testRecord.matchingTestScore
        })
      )
    })
    .map(item => {
      return {
        studentTestScoreDcid: item.studentTestScore,
        benchmark: item.testResult['Assessment Measure-Composite Score-Levels']
      };
    });
}

/**
 * @return {Observable} Observable that emits data matching the format
 * asked by the user
 */
function createTransformer(config, sourceObservable) {
  if (config.prompt.table === 'Test Results') {
    return testResultsTransform(config, sourceObservable);
  } else if (config.prompt.table === 'U_StudentTestProficiency') {
    return proficiencyTransform(config, sourceObservable);
  }
}

/**
 * maps objects emitted by srcObservable to a new Observable that
 * converts those objects to csv strings and emits the results
 *
 * @param  {object}     config
 * @param  {Observable} srcObservable
 * @return {Disposable}
 */
function csvObservable(config, srcObservable) {
  var ws;
  let csvObservable = srcObservable.concatMap(async function (item, i) {
    if (i === 0) {
      let outputFilename = `output/rogl/${basename(config.inputFile, extname(config.inputFile)) }-${config.prompt.table}${extname(config.inputFile) }`;
      console.log(outputFilename);

      // creates file if it doesn't exist
      ws = createWriteStream(outputFilename, {
        flags: 'a'
      });

      await fs.truncate(outputFilename);
    }
    return toCSV({
        data: item,
        del: '\t',
        hasCSVColumnTitle: i === 0 // print columns only if this is the first item emitted
      })
      .then(csvStr => {
        let csvRemQuotes = csvStr.replace(/"/g, '');

        // Add a newline character before every line except the first line
        let csv = i === 0 ? csvRemQuotes : EOL + csvRemQuotes;
        console.log('csv == ' + csv);
        return csv;
      });
  });

  return csvObservable.subscribe(item => ws.write(item));
}
