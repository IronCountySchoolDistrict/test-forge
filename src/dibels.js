require('babel-polyfill');

import {
  getStudentIdFromStudentNumber,
  getMatchingStudentTest,
  getMatchingStudentTestScore,
  getMatchingProficiency
}
from './service';
import {
  createWriteStream
}
from 'fs';

import fs from 'fs-promise';
import {
  Observable
}
from '@reactivex/rxjs';
import Promise from 'bluebird';
import {
  gustav
}
from 'gustav';
import {
  basename, extname
}
from 'path';
import json2csv from 'json2csv';
import {
  EOL
}
from 'os';
import {
  logger
}
from './index';
import util from 'util';
import {
  isEmpty
}
from 'lodash';

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
      return Observable.zip(
        Observable.of(item),

        Observable.fromPromise(fs.readFile('./config.json')),

        (s1, s2) => {
          return {
            testResult: s1,
            config: JSON.parse(s2.toString())
          };
        }
      )
    })
    .flatMap(item => {
      // Check for matchning student test score.
      let matchingTestScore = getMatchingStudentTestScore(
          item.testResult['Student Primary ID'],
          item.testResult['School Year'],
          item.testResult['Composite Score'],
          config.prompt.testId
        )
        .then(r => {
          // Expecting there to NOT be any matching student test score Record
          if (r.rows.length) {
            throw {
              studentTestScore: r,
              testResult: item.testResult,
              message: `expected getMatchingStudentTestScore to return 0 records, got ${r.rows.length} rows`
            };
          } else {
            return false;
          }
        });

      let matchingTestScoreObservable = Observable.fromPromise(
        matchingTestScore
      ).catch(e => {
        logger.log('info', `Error checking for matching student test records for student_number: ${item['Student Primary ID']}`, {
          psDbError: util.inspect(e, {
            showHidden: false,
            depth: null
          })
        });
        return Observable.of({});
      });

      return Observable.zip(
        matchingTestScoreObservable,
        Observable.of(item),

        (s1, s2) => {
          return {
            hasMatchingStudentTestScore: s1,
            testResult: s2.testResult,
            config: s2.config
          };
        }
      );

    })
    .filter(item => {
      // if item.hasMatchingStudentTestScore === {}, do not emit that item
      return !isEmpty(item.hasMatchingStudentTestScore);
    })
    .flatMap(item => {
      console.log('flatMap item == %j', item);
      return Observable.zip(

        Observable.fromPromise(
          getStudentIdFromStudentNumber(item.testResult['Student Primary ID'])
        ),

        Observable.of(item),

        (s1, s2) => ({
          'Test Date': s2.config.testConstants.ROGL_Begin_Year.testDate,
          'Student Id': s1,
          'Student Number': s2.testResult['Student Primary ID'],
          'Grade Level': s2.testResult.Grade,
          'Composite Score Alpha': s2.testResult['Composite Score']
        })
      );
    });

}

function proficiencyTransform(config, observable) {
  return observable
    .flatMap(item => {
      let matchingTestScore = getMatchingStudentTestScore(
          item['Student Primary ID'],
          item['School Year'],
          item['Composite Score'],
          config.prompt.testId
        )
        .then(r => {
          // Expecting there to NOT be any matching student test score Record
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
            psDbError: util.inspect(e, {
              showHidden: false,
              depth: null
            })
          });
          logger.log('info', `dibels record for student_number: ${item['Student Primary ID']}`, {
            sourceData: util.inspect(item, {
              showHidden: false,
              depth: null
            })
          });
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
            psDbError: util.inspect(e, {
              showHidden: false,
              depth: null
            })
          });
          logger.log('info', `dibels record for student_number: ${item['Student Primary ID']}`, {
            sourceData: util.inspect(item, {
              showHidden: false,
              depth: null
            })
          });
          return Observable.of({});
        }),

        Observable.of(item),

        (s1, s2) => ({
          testResult: s2.record,
          studentTestScore: s2.studentTestScore,
          proficiency: s1
        })

      )
    })
    .filter(item => {
      // if item.hasMatchingStudentTestScore === {}, do not emit that item
      return !isEmpty(item.proficiency);
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
    console.log('creating test Results');
    return testResultsTransform(config, sourceObservable);
  } else if (config.prompt.table === 'U_StudentTestProficiency') {
    return proficiencyTransform(config, sourceObservable);
  }
}

/**
 * maps objects emitted by srcObservable to a new Observable that
 * converts those objects to csv strings and emits the results
 * @param  {Observable}
 * @return {Disposable}
 */
function csvObservable(config, srcObservable) {
  var ws;
  let csvObservable = srcObservable.concatMap(async function(item, i) {
    if (i === 0) {
      /*let outputFilename = `output/${toFileName(item.extra.testProgramDesc)}.txt`;*/
      let outputFilename = `output/${basename(config.file, extname(config.file))}-${config.prompt.table}${extname(config.file)}`;
      console.log('outputFilename == ', outputFilename);
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
