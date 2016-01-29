require('babel-polyfill');

import { createWriteStream, truncateSync } from 'fs';
import Promise from 'bluebird';
import fs from 'fs-promise';
import { Observable } from '@reactivex/rxjs';
import { isEmpty } from 'lodash';
import { gustav } from 'gustav';
import json2csv from 'json2csv';
import { EOL } from 'os';

import { getStudentIdFromSsid, getStudentNumberFromSsid, getMatchingStudentTestScore, getTestFromName } from './service';
import { logger } from './index';
import { printObj } from './util';

var toCSV = Promise.promisify(json2csv);

export function createWorkflow(sourceObservable, prompt) {
  var config = {
    prompt: prompt
  };

  gustav.source('dataSource', () => sourceObservable);

  return gustav.createWorkflow()
    .source('dataSource')
    .transf(transformer, config)
    .sink(crtCsvSink, config);
}

function transformer(config, observer) {
  if (config.prompt.table === 'Test Results') {
    return testResultsTransform(observer);
  }
  if (config.prompt.table === 'U_StudentTestProficiency') {
    return proficiencyTransform(observer);
  }
}

function testResultsTransform(observer) {
  return observer
    .map(item => {
      item.test_date = new Date(`05/01/${item.Schoolyear}`).toLocaleDateString();
      return item;
    })
    .map(item => {
      // Convert "Earth Science" to "Earth Systems Science"
      if (item.TestName === 'Earth Science') {
        item.TestName = 'Earth Systems Science';
      }
      return item;
    })
    .flatMap(item => {

      let studentNumberObs = Observable.fromPromise(getStudentNumberFromSsid(item.ssid))
        .catch(e => {
          logger.log('info', `Error fetching student number for ssid: ${item}`, {
            psDbError: printObj(e)
          });
          logger.log('info', 'Sage Record: ', {
            sourceData: printObj(item)
          });
          return Observable.of({});
        });

      let studentIdObs = Observable.fromPromise(getStudentIdFromSsid(item.ssid))
        .catch(e => {
          logger.log('info', `Error fetching student id for ssid: ${item.ssid}`, {
            psDbError: printObj(e)
          });
          logger.log('info', 'Sage Record: ', {
            sourceData: printObj(item)
          });
          return Observable.of({});
        });

      let matchingTestsObs = Observable.fromPromise(getTestFromName(`EOL - ${item.TestName}`))
        .map(matchingTests => {
          if (!(matchingTests.rows.length === 1)) {
            throw {
              studentTestScore: matchingTests,
              testResult: item,
              message: `expected getTestFromName to return 1 record, got ${matchingTests.rows.length} rows`
            };
          } else {
            return matchingTests.rows[0].ID
          }
        })
        .catch(e => {
          logger.log('info', `Error finding matching test for ssid: ${item.ssid}`, {
            psDbError: printObj(e)
          });
          logger.log('info', 'Sage Record: ', {
            sourceData: printObj(item)
          });
          return Observable.of(0);
        })
        .filter(matchingTest => !!matchingTest)

      return Observable.zip(
        studentNumberObs,
        studentIdObs,
        matchingTestsObs,

        (studentNumber, studentId, matchingTest) => ({
          studentNumber: studentNumber,
          studentId: studentId,
          matchingTestId: matchingTest,
          testResult: item
        })
      );
    })
    .flatMap(item => {
      let fullSchoolYear = toFullSchoolYear(item.testResult.school_year);
      let matchingTestScore = getMatchingStudentTestScore(
        item.studentNumber,
        fullSchoolYear,
        item.testResult.test_overall_score,
        item.matchingTestId)
        .then(r => {
          // Expecting there to NOT be any matching student test score record,
          // so if there is one or more, throw an exception
          if (r.rows.length) {
            const error = {
              studentTestScore: r,
              testResult: item.testResult,
              message: `expected getMatchingStudentTestScore to return 0 records, got ${r.rows.length} rows`
            };

            logger.log('info', `Error checking for matching student test records for ssid: ${item.ssid}`, {
              psDbError: printObj(error)
            });
            
            return r;
          } else {
            return null;
          }
        });

      return Observable.fromPromise(matchingTestScore)
        .filter(r => !r)
        .map(_ => {
          return {
            'csvOutput': {
              'Test Date': item.testResult.test_date,
              'Student Id': item.studentId,
              'Student Number': item.studentNumber,
              'Grade Level': item.testResult.GradeLevel,
              'Composite Score Alpha': item.testResult.ScaleScore
            },
            'extra': {
              testName: item.testResult.TestName
            }
          };
        })
    })
}

function proficiencyTransform(observer) {
  return observer
    .map(item => {
      // If test_program_desc is spelled wrong, replace that value with the correctly spelled value
      item.test_program_desc = item.test_program_desc === 'Earth Sytems Science' ? 'Earth Systems Science' : item.test_program_desc
      return item;
    })
    .flatMap(item => {

      let studentNumberObs = Observable.fromPromise(getStudentNumberFromSsid(item.ssid))
        .catch(e => {
          logger.log('info', `Error fetching student number for ssid: ${item.ssid}`, {
            psDbError: printObj(e)
          });
          logger.log('info', `SAMS DB Record for student_test_id: ${item.student_test_id}`, {
            sourceData: printObj(item)
          });
          return Observable.of({});
        });

      let studentIdObs = Observable.fromPromise(getStudentIdFromSsid(item.ssid))
        .catch(e => {
          logger.log('info', `Error fetching student id for ssid: ${item.ssid}`, {
            psDbError: printObj(e)
          });
          logger.log('info', `SAMS DB Record for student_test_id: ${item.student_test_id}`, {
            sourceData: printObj(item)
          });
          return Observable.of({});
        });

      let matchingTestsObs = Observable.fromPromise(getTestFromName(item.test_program_desc))
        .map(matchingTests => {
          if (!(matchingTests.rows.length === 1)) {
            throw {
              studentTestScore: matchingTests,
              testResult: item,
              message: `expected getTestFromName to return 1 record, got ${matchingTests.rows.length} rows`
            };
          } else {
            return matchingTests.rows[0].ID
          }
        })
        .catch(e => {
          logger.log('info', `Error finding matching test for ssid: ${item.ssid}`, {
            psDbError: printObj(e)
          });
          logger.log('info', `SAMS DB Record for student_test_id: ${item.student_test_id}`, {
            sourceData: printObj(item)
          });
          return Observable.of(0);
        })
        .filter(matchingTest => !!matchingTest)

      return Observable.zip(
        studentNumberObs,
        studentIdObs,
        matchingTestsObs,

        (studentNumber, studentId, matchingTest) => ({
          studentNumber: studentNumber,
          studentId: studentId,
          matchingTestId: matchingTest,
          testResult: item
        })
        );
    })
    .flatMap(item => {

      let fullSchoolYear = toFullSchoolYear(item.testResult.school_year);
      let matchingTestScore = getMatchingStudentTestScore(
        item.studentNumber,
        fullSchoolYear,
        item.testResult.test_overall_score,
        item.matchingTestId)
        .then(r => {
          // Expecting there to NOT be any matching student test score record,
          // so if there is one or more, throw an exception
          if (!r.rows.length) {
            const error = {
              studentTestScore: r,
              testResult: item.testResult,
              message: `expected getMatchingStudentTestScore to return 1 records, got ${r.rows.length} rows`
            };

            logger.log('info', `Error checking for matching student test records for ssid: ${item.ssid}`, {
              psDbError: printObj(error)
            });
          } else {
            return r.rows[0].DCID;
          }
        });

      return Observable.fromPromise(matchingTestScore)
        .filter(r => !!r)
        .map(matchingTestScoreDcid => {
          return {
            csvOutput: {
              studentTestScoreDcid: matchingTestScoreDcid,
              proficiency: item.testResult.ProficiencyLevel
            },
            extra: {
              testProgramDesc: item.testResult.test_program_desc
            }
          };
        });
    })
}

/**
 * converts a school year in the format "2011" to "2010-2011"
 * @param  {string|number} shortSchoolYear
 * @return {string}
 */
function toFullSchoolYear(shortSchoolYear) {
  return `${shortSchoolYear - 1}-${shortSchoolYear}`;
}

function crtCsvSink(config, observable) {
  return observable
    .groupBy(x => x.extra.testName)
    .subscribe(groupedObservable => {

      let ws;
      toCsvObservable(config, groupedObservable).subscribe(
        item => {
          if (!ws && item.ws) {
            ws = item.ws;
          }
          ws.write(item.csv);
        },

        error => console.log('error == ', error),

        () => {
          ws.end();
        }
        );
    });
}



/**
 * maps objects emitted by srcObservable to a new Observable that
 * converts those objects to csv strings and emits the results
 * @param  {GroupedObservable} observable source Observable
 * @return {object}
 *      	{
 *     			observable: // new observable that emits csv data derived from @param srcObservable
 *         	ws: writable file stream to write results to file system
 *     		}
 */
function toCsvObservable(config, srcObservable) {
  return srcObservable.concatMap(function (item, i) {
    if (i === 0) {
      console.log('getting outputFilename');
      let outputFilename = `output/sage/EOL - ${item.extra.testName}-${config.prompt.table}.txt`;

      // creates file if it doesn't exist
      var ws = createWriteStream(outputFilename, {
        flags: 'a'
      });
      console.log(`created write stream for ${outputFilename}`);

      try {
        ws.on('open', function (fd) {
          truncateSync(outputFilename);
        });
      } catch (e) {
        console.log('couldnt truncate file');
        console.log('e == %j', e);
      }
    }
    return toCSV({
      data: item.csvOutput,
      del: '\t',
      hasCSVColumnTitle: i === 0 // print columns only if this is the first item emitted
    })
      .then(csvStr => {
        let csvRemQuotes = csvStr.replace(/"/g, '');

        // Add a newline character before every line except the first line
        let csvVal = i === 0 ? csvRemQuotes : EOL + csvRemQuotes;
        if (ws) {
          return {
            csv: csvVal,
            ws: ws
          };
        } else {
          return {
            csv: csvVal
          };
        }
      });

  });
}
