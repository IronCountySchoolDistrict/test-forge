// Wraps database queries found in service.js with business logic
import {
  getStudentNumberFromSsid,
  getStudentIdFromSsid,
  getStudentIdFromStudentNumber,
  getTestFromName,
  getMatchingStudentTestScore,
  getMatchingProficiency
}
from './service';
import { printObj } from './util';
import { logger } from './index';
import { Observable } from '@reactivex/rxjs';

function logErrors(item, msg, e) {
  logger.log('info', msg, {
    psDbError: printObj(e)
  });
  logger.log('info', 'Source Data Record: ', {
    sourceData: printObj(item)
  });
}

export function ssidToStudentNumber(ssid, item) {
  return Observable.fromPromise(getStudentNumberFromSsid(ssid))
    .map(studentNumber => {
      if (studentNumber.rows.length !== 1) {
        throw {
          error: new Error(`Expected getStudentNumberFromSsid() to return only one row, got back ${studentNumber.rows.length} records`),
          response: studentNumber
        };
      } else {
        try {
          return studentNumber.rows[0].STUDENT_NUMBER;
        } catch (e) {
          throw {
            error: e,
            response: studentNumber
          };
        }
      }
    })
    .catch(e => {
      logErrors(item, `Error fetching student number for ssid: ${item.ssid}`, e);
      return Observable.of({});
    })
    .filter(studentNumber => !!studentNumber);
}

export function ssidToStudentId(ssid, item) {
  return Observable.fromPromise(getStudentIdFromSsid(ssid))
    .map(studentId => {
      if (studentId.rows.length !== 1) {
        throw {
          error: new Error(`Expected getStudentIdFromSsid() to return only one row, got back ${studentId.rows.length} records`),
          response: studentId
        };
      } else {
        try {
          return studentId.rows[0].ID;
        } catch (e) {
          throw {
            error: e,
            response: studentId
          };
        }
      }
    })
    .catch(e => {
      logErrors(item, `Error fetching student id for ssid: ${item.ssid}`, e);
      return Observable.of({});
    })
    .filter(studentId => !!studentId);
}

export function studentNumberToStudentId(studentNumber, item) {
  return Observable.fromPromise(getStudentIdFromStudentNumber(studentNumber))
    .map(studentId => {
      if (studentId.rows.length > 1) {
        throw {
          error: new Error(`Expected getStudentIdFromStudentNumber() to return one row, got back ${r.rows.length} records`),
          response: studentId
        };
      } else {
        try {
          return studentId.rows[0].ID;
        } catch (e) {
          throw {
            error: e,
            response: studentId
          };
        }
      }
    })
    .catch(e => {
      logErrors(item, `Error fetching student id for ssid: ${item.ssid}`, e);
      return Observable.of({});
    })
    .filter(studentId => !!studentId);
}

export function testNameToDcid(testName, item) {
  return Observable.fromPromise(getTestFromName(`EOL - ${testName}`))
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
      logErrors(item, `Error finding matching test in PowerSchool for test name: EOL - ${item.TestName}`, e);
      return Observable.of(0);
    })
    .filter(matchingTest => !!matchingTest);
}

/**
 * checks for existing StudentTestScore records that match the `item` record passed in
 * @return {observable}
 */
export function studentTestScoreDuplicateCheck(studentNumber, fullSchoolYear, testScore, testId, item) {
  return Observable.fromPromise(
      getMatchingStudentTestScore(
        studentNumber,
        fullSchoolYear,
        testScore,
        testId
      )
    )
    .map(studentTestScore => {
      // Expecting there to NOT be any matching student test score record,
      // so if there is one or more, throw an exception
      if (studentTestScore.rows.length) {
        throw {
          studentTestScore: studentTestScore,
          testResult: item.testResult,
          message: `expected checkDuplicateStudentTestScore to return 0 records, got ${studentTestScore.rows.length} rows`
        };
      } else {
        return null; // Returning a truthy value will allow the current record to be processed
      }
    })
    .catch(e => {
      logErrors(item, `Error checking for matching student test records for studentNumber: ${studentNumber}`, e);
      return Observable.of(true);
    })
    .filter(studentTestScore => !studentTestScore);
}


export function proficiencyDuplicateCheck(studentNumber, fullSchoolYear, testScore, testId, item) {
  return Observable.fromPromise(
      getMatchingProficiency(
        studentNumber,
        fullSchoolYear,
        testScore,
        testId
      )
    )
    .map(proficiency => {
      if (proficiency.rows.length) {
        throw {
          proficiencyRecord: proficiencyRecord,
          testResult: item.testResult,
          message: `expected proficiencyDuplicateCheck to return 0 records, got ${proficiency.rows.length} rows`
        }
      }
    })
    .catch(e => {
      logger.log('info', `Error fetching matching student proficiency record for student_number: ${studentNumber}`, {
        psDbError: printObj(e)
      });
      logger.log('info', `source record for student_number: ${studentNumber}`, {
        sourceData: printObj(item)
      });
      return Observable.of(true);
    })
    .filter(matchingProficiency => !matchingProficiency);
}

export function testRecordToMatchingDcid(studentNumber, fullSchoolYear, testScore, testId, item) {
  return Observable.fromPromise(
      getMatchingStudentTestScore(
        studentNumber,
        fullSchoolYear,
        testScore,
        testId
      )
    )
    .map(studentTestScore => {
      // Expecting there to NOT be any matching student test score record,
      // so if there is one or more, throw an exception
      if (studentTestScore.rows.length !== 1) {
        throw {
          studentTestScore: studentTestScore,
          testResult: item.testResult,
          message: `expected getMatchingStudentTestScore to return 1 record, got ${studentTestScore.rows.length} rows`
        };
      } else {
        return studentTestScore.rows[0].DCID;
      }
    })
    .catch(e => {
      logErrors(item, `Error looking for matching student test record for studentNumber: ${studentNumber}`, e);
      return Observable.of(null);
    })
    .filter(studentTestScore => !!studentTestScore);
}
