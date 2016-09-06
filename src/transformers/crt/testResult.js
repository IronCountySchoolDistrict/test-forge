import { uniqWith, isEqual } from 'lodash';
import { Observable } from '@reactivex/rxjs';
import {
  createTestDate,
  correctTestProgramDesc,
  correctConceptDesc,
  toFullSchoolYear,
  groupBy,
  flatten,
  mergeGroups
} from './util';
import { studentTestToConceptResults, studentTestScoreDuplicateCheck } from '../../blogic';
import { getStudentIdsFromSsidBatchDual, getTestIdsFromNamesBatch } from '../../service';

/**
 * merge ssid into all testResult objects
 * @param  {array[object]} testResults
 * @return {observable}
 */
function mergeSsidAndTest(testResults) {
  const distinctSsids = uniqWith(testResults, (a, b) => a.ssid === b.ssid)
    .map(item => item.ssid);
  const distinctTestNames = uniqWith(testResults, (a, b) => a.test_program_desc === b.test_program_desc)
    .map(item => item.test_program_desc)
    .map(testProgramDesc => `EOL - ${testProgramDesc}`);

  return Observable.zip(
    getStudentIdsFromSsidBatchDual(distinctSsids),
    getTestIdsFromNamesBatch(distinctTestNames),

    (studentIds, testIds) => {
      let studentIdGroups = groupBy(item => parseInt(item.ssid), testResults);
      studentIdGroups = mergeGroups(
        studentIdGroups,
        studentIds,
        item => parseInt(item.ssid),
        item => !item.student_id || !item.student_number
      );
      let mergedFlattenedStudentIds = flatten(studentIdGroups);

      let testNameGroups = groupBy(item => `EOL - ${item.test_program_desc}`, mergedFlattenedStudentIds);
      testNameGroups = mergeGroups(
        testNameGroups,
        testIds,
        item => item.test_name,
        item => !item.test_id
      );
      let mergedFlattendedTestNames = flatten(testNameGroups);
      return flatten(studentIdGroups);
    }
  );
}


function mergeTestResultConcept(testResult) {
  return Observable.zip(
    studentTestToConceptResults(testResult.student_test_id, testResult).bufferCount(100),

    (testResultConcepts) => {
      let distinctTestResultConcepts = testResultConcepts
        .map(testResultConcept => {
          testResultConcept.test_program_desc = correctTestProgramDesc(testResultConcept.test_program_desc);
          return testResultConcept;
        })
        .map(testResultConcept => {
          testResultConcept.concept_desc = correctConceptDesc(testResultConcept.concept_desc);
          return testResultConcept
        });
      distinctTestResultConcepts = uniqWith(distinctTestResultConcepts, isEqual);
      testResult.resultConcepts = distinctTestResultConcepts;
      return testResult;
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
    .map(_ => {
      let finalObject = {
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
      };
      if (testResult.resultConcepts) {
        testResult.resultConcepts = uniqWith(testResult.resultConcepts, isEqual);
        testResult.resultConcepts.forEach(resultConcept => {
          finalObject.csvOutput[`${resultConcept.concept} Percent`] = resultConcept.score;
        });
      }

      return finalObject;
    });
}

export function testResultTransform(observable) {
  return observable
    .map(correctTestProgramDesc)
    .map(createTestDate)
    .bufferCount(500)
    .flatMap(mergeSsidAndTest)
    .flatMap(item => Observable.from(item))
    .flatMap(checkForDuplicatesAndCreateFinalObject);
}
