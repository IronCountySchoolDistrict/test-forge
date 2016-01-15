require('babel-polyfill');

import { createWriteStream, truncateSync } from 'fs';
import Promise from 'bluebird';
import fs from 'fs-promise';
import { Observable } from '@reactivex/rxjs';
import { isEmpty } from 'lodash';
import { gustav } from 'gustav';
import json2csv from 'json2csv';
import { EOL } from 'os';

import { getStudentIdFromSsid, getStudentNumberFromSsid, getTestDcid, getMatchingStudentTestScore, getMatchingTests } from './service';
import { logger } from './index';
import { printObj } from './util';

var toCSV = Promise.promisify(json2csv);

function testResultsTransform(observer) {
  return observer
    .map(item => {
      item.test_date = new Date(`05/01/${item.school_year}`).toLocaleDateString();
      return item;
    })
    .flatMap(item => {

      let studentNumberObs = Observable.fromPromise(getStudentNumberFromSsid(item.ssid))
        .catch(e => {
          logger.log('info', `Error fetching student number for ssid: ${item}`, {
            psDbError: printObj(e)
          });
          logger.log('info', 'SAMS DB Record for item:', {
            sourceData: printObj(item)
          });
          return Observable.of({});
        });

      let studentIdObs = Observable.fromPromise(getStudentIdFromSsid(item.ssid))
        .catch(e => {
          logger.log('info', `Error fetching student id for ssid: ${item.ssid}`, {
            psDbError: printObj(e)
          });
          logger.log('info', 'SAMS DB Record for item: ', {
            sourceData: printObj(item)
          });
          return Observable.of({});
        });

      let matchingTestsObs = Observable.fromPromise(getMatchingTests(item.TestName))
        .map(matchingTests => {
          if (!(matchingTests.rows.length === 1)) {
            throw {
              studentTestScore: matchingTests,
              testResult: item,
              message: `expected getMatchingTests to return 1 record, got ${matchingTests.rows.length} rows`
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
          if (r.rows.length) {
            const error = {
              studentTestScore: r,
              testResult: item.testResult,
              message: `expected getMatchingStudentTestScore to return 0 records, got ${r.rows.length} rows`
            };

            logger.log('info', `Error checking for matching student test records for ssid: ${item.ssid}`, {
              psDbError: printObj(error)
            });
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
              'Grade Level': item.testResult.grade_level,
              'Composite Score Alpha': item.testResult.test_overall_score
            },
            'extra': {
              testProgramDesc: item.testResult.test_program_desc,
              studentTestId: item.testResult.student_test_id
            }
          };
        })
    })
}