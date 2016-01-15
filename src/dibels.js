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

import { getStudentIdFromStudentNumber, getMatchingStudentTest, getMatchingStudentTestScore, getMatchingProficiency } from './service';
import { printObj } from './util';
import { logger } from './index';

var toCSV = Promise.promisify(json2csv);

export function createWorkflow(sourceObservable, prompt, file) {
  gustav.source('dibelsSource', () => sourceObservable);
  
  let config = {
    prompt: prompt,
    file: file
  };
  return gustav.createWorkflow()
    .source('dibelsSource')
    .transf(createTransformer, config)
    .sink(csvObservable, config);
}

function testResultsTransform(config, observable) {
  return observable
    .flatMap(item => {
      const configFile = fs.readFile('./config.json')
        .then(r => JSON.parse(r.toString()))

      const matchingTestScore = getMatchingStudentTestScore(
        item['Student Primary ID'],
        item['School Year'],
        item['Composite Score'],
        config.prompt.testId
        )
        .then(r => {
          // Expecting there to NOT be any matching student test score Record
          if (r.rows.length) {
            const error = {
              studentTestScore: r,
              testResult: item.testResult,
              message: `expected getMatchingStudentTestScore to return 0 records, got ${r.rows.length} rows`
            };

            logger.log('info', `Error checking for matching student test records for student_number: ${item['Student Primary ID']}`, {
              psDbError: printObj(error)
            });

            return r.rows;
          } else {
            return {};
          }
        });

      let studentId = getStudentIdFromStudentNumber(item['Student Primary ID'])

      let allPromises = Promise.all([configFile, matchingTestScore, studentId])
        .then(r => {
          return {
            config: r[0],
            matchingTestScore: r[1],
            studentId: r[2]
          }
        });


      let promisesObs = Observable.fromPromise(allPromises);

      let testResultObs = Observable.of({
        testResult: item
      });

      return Observable.zip(testResultObs, promisesObs, merge);

    })
    .filter(item => {

      // return true (allow through filter) if the following criteria are met:
      // 1) no matching student test score record was found
      // 2) the config file object is not empty
      // 3) studentId is not falsey (not found)
      return !item.matchingTestScore.length &&
        !isEmpty(item.config) &&
        !!item.studentId;
    })
    .map(item => {
      return {
        'Test Date': item.config.testConstants.ROGL_Begin_Year.testDate,
        'Student Id': item.studentId,
        'Student Number': item.testResult['Student Primary ID'],
        'Grade Level': item.testResult.Grade,
        'Composite Score Alpha': item.testResult['Composite Score']
      }
    });

}

function proficiencyTransform(config, observable) {
  return observable
    .flatMap(item => {
      const matchingTestScore = getMatchingStudentTestScore(
        item['Student Primary ID'],
        item['School Year'],
        item['Composite Score'],
        config.prompt.testId
        )
        .then(r => {
          // Expecting there to be 1 matching student test score record
          if (!r.rows.length) {
            throw {
              studentTestScore: r,
              testResult: item,
              message: `expected getMatchingStudentTestScore to return 1 record, got ${r.rows.length} rows`
            };
          } else {
            return r.rows[0].DCID;
          }
        });

      return Observable.zip(
        Observable.fromPromise(matchingTestScore)
          .catch(e => {
            logger.log('info', `Error fetching matching student test score for student_number: ${item['Student Primary ID']}`, {
              psDbError: printObj(e)
            });
            logger.log('info', `dibels record for student_number: ${item['Student Primary ID']}`, {
              sourceData: printObj(item)
            });
            return Observable.of({});
          }),

        Observable.of(item),

        (s1, s2) => ({
          matchingTestScore: s1,
          record: s2
        })
        );

    })
    .flatMap(item => {
      let matchingProficiency = getMatchingProficiency(
        item.record['Student Primary ID'],
        item.record['School Year'],
        item.record['Composite Score'],
        config.prompt.testId
        )
        .then(r => {
          if (r.rows.length) {
            throw {
              studentTestScore: r,
              testResult: item.record,
              message: `expected getMatchingProficiency to return 0 records, got ${r.rows.length} rows`
            };
          }
        });

      return Observable.zip(
        Observable.fromPromise(matchingProficiency)
          .catch(e => {
            logger.log('info', `Error fetching matching student proficiency record for student_number: ${item['Student Primary ID']}`, {
              psDbError: printObj(e)
            });
            logger.log('info', `dibels record for student_number: ${item['Student Primary ID']}`, {
              sourceData: printObj(item)
            });
            return Observable.of({});
          }),

        Observable.of(item),

        (s1, s2) => ({
          testResult: s2.record,
          studentTestScore: s2.matchingTestScore,
          proficiency: s1
        })
        )
    })
    .filter(item => {
      // if item.proficiency === {}, no matching proficiency records were found, so allow it through
      // if item.proficiency is not empty, existing proficiency record was found, so do not allow it through
      return isEmpty(item.proficiency);
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
  let csvObservable = srcObservable.concatMap(async function(item, i) {
    if (i === 0) {
      let outputFilename = `output/${basename(config.file, extname(config.file)) }-${config.prompt.table}${extname(config.file) }`;
      
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
        return csv;
      });
  });

  return csvObservable.subscribe(item => {
    ws.write(item);
  });
}
