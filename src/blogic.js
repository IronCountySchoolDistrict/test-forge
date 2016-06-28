// Wraps database queries found in service.js with business logic
import {
  getStudentNumberFromSsid,
  getStudentIdFromSsid,
  getStudentIdFromStudentNumber,
  getTestFromName,
  getMatchingStudentTestScore,
  getMatchingProficiency,
  getStudentIdsFromSsidBatchDual
} from './service';
import cache from 'memory-cache';
import { printObj } from './util';
import { logger } from './index';
import { Observable } from '@reactivex/rxjs';
import { merge } from 'lodash';

function logErrors(item, msg, e) {
  logger.log('info', msg, {
    psDbError: printObj(e)
  });
  if (item) {
    logger.log('info', 'Source Data Record: ', {
      sourceData: printObj(item)
    });
  }
}

/**
 *
 * @param ssids {array}
 */
export function ssidsToStudentIds(ssids) {
  return Observable.fromPromise(getStudentIdsFromSsidBatchDual(ssids))
    .flatMap(studentIds => {
      const mergedStudentIds = ssids
        .map(ssid => {
          const matchingStudentIdItem = studentIds.rows.filter(studentId => {
            return parseInt(studentId.ssid) === ssid
          });
          if (matchingStudentIdItem.length) {
            return {
              ssid: ssid,
              studentId: matchingStudentIdItem[0].student_id,
              studentNumber: matchingStudentIdItem[0].student_number
            };
          } else {
            let errorObj = {
              error: new Error(`Expected to find a student's ssid from Powerschool that matches an ssid from the source database.`),
              ssid: ssid
            };
            logErrors(null, `Error: `, errorObj);
            return null;
          }
        })
        .filter(item => !!item);
      //mergedArray still contains null elements that should be filtered
      return Observable.of(mergedStudentIds);
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
      return Observable.of(0);
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
      return Observable.of(0);
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
      return Observable.of(null);
    })
    .filter(studentId => !!studentId);
}

export function testNameToDcid(testName, item) {
  let cachedTestDcid = cache.get(`EOL - ${testName}`);
  if (cachedTestDcid) {
    return Observable.of(cachedTestDcid);
  } else {
    let fullTestName = `EOL - ${testName}`;
    return Observable.fromPromise(getTestFromName(fullTestName))
      .map(matchingTests => {
        if (!(matchingTests.rows.length === 1)) {
          throw {
            studentTestScore: matchingTests,
            testResult: item,
            message: `expected getTestFromName to return 1 record, got ${matchingTests.rows.length} rows`
          };
        } else {
          cache.put(`EOL - ${testName}`, matchingTests.rows[0].ID);
          return matchingTests.rows[0].ID;
        }
      })
      .catch(e => {
        logErrors(item, `Error finding matching test in PowerSchool for test name: ${fullTestName}`, e);
        return Observable.of(null);
      })
      .filter(matchingTest => !!matchingTest);
  }
}

/**
 * checks for existing StudentTestScore records that match the `item` record passed in
 * @return {observable}
 */
export function studentTestScoreDuplicateCheck(studentNumber, fullSchoolYear, testScore, testId, scoreName, item) {
  return Observable.fromPromise(
      getMatchingStudentTestScore(
        studentNumber,
        fullSchoolYear,
        testScore,
        testId,
        scoreName
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

/**
 * checks for existing StudentTestScore records that match the `item` record passed in
 * @return {observable}
 */
export function studentTestResultConceptSDuplicateCheck(studentNumber, fullSchoolYear, testScore, testId, scoreName, item) {
  return Observable.fromPromise(
      getMatchingStudentTestScore(
        studentNumber,
        fullSchoolYear,
        testScore,
        testId,
        scoreName
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
