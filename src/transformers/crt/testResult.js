import { uniqWith } from 'lodash';
import { Observable } from '@reactivex/rxjs';
import {
  createTestDate,
  correctTestProgramDesc,
  toFullSchoolYear,
  groupBy,
  flatten,
  mergeGroups
} from './util';
import { getStudentIdsFromSsidBatchDual } from '../../blogic';

/**
 * merge ssid into all testResult objects
 * @param  {array[object]} testResults
 * @return {observable}
 */
function mergeSsidAndTest(testResults) {
  const distinctSsids = uniqWith(testResults, (a, b) => a.ssid === b.ssid)
    .map(item => item.ssid);
  const distinctTestNames = uniqWith(bufferedStudentTestConceptResults, (a, b) => a.test_program_desc === b.test_program_desc)
    .map(item => item.test_program_desc)
    .map(testProgramDesc => `EOL - ${testProgramDesc}`);

  return Observable.zip(
    getStudentIdsFromSsidBatchDual(distinctSsids),
    getTestIdsFromNamesBatch(distinctTestNames),

    (studentIds, testIds) => {
      const studentIdGroups = groupBy(item => parseInt(item.ssid), testResults);
      const testNameGroups = groupBy(item => `EOL - ${item.test_program_desc}`, testResults);

      mergeGroups(studentIdGroups, studentIds, item => parseInt(item.ssid));
      mergeGroups(testNameGroups, testIds, item => item.test_name);

      return flatten(studentIdGroups);
    }
  );
}

function checkForDuplicatesAndCreateFinalObject(testResult) {
  let fullSchoolYear = toFullSchoolYear(testResult.school_year);

  let matchingTestScore = studentTestScoreDuplicateCheck(
    testResult.student_number,
    fullSchoolYear,
    testResult.test_overall_score,
    testResult.test_id,
    'Composite',
    testResult
  );

  return matchingTestScore
    .map(_ => ({
      csvOutput: {
        'Test Date': testResult.test_date,
        'Student Id': testResult.student_id,
        'Student Number': testResult.student_number,
        'Grade Level': testResult.grade_level,
        'Composite Score Num': testResult.test_overall_score
      },
      'extra': {
        testProgramDesc: testResult.test_program_desc,
        studentTestId: testResult.student_test_id
      }
    }));
}

export function testResultTransform(observable) {
  return observer
    .map(correctTestProgramDesc)
    .map(createTestDate)
    .bufferCount(500)
    .flatMap(mergeSsidandTest)
    .flatMap(item => Observable.from(item))
    .flatMap(checkForDuplicatesAndCreateFinalObject);
}
