import { uniqWith } from 'lodash';
import { Observable } from '@reactivex/rxjs';
import { getStudentIdsFromSsidBatchDual, getTestIdsFromNamesBatch } from '../../service';
import { studentTestScoreDuplicateCheck } from '../../blogic';
import {
  correctConceptDesc,
  correctTestProgramDesc,
  createTestDate,
  groupBy,
  flatten,
  mergeGroups,
  toFullSchoolYear
} from './util';

/**
 * fix test_program_desc and concept_desc spelling errors and inconsistencies
 * @param {object} studentTestConceptResult row of crt data with student test concepts results
 * @param {string} studentTestConceptResult.test_program_desc name of test
 * @param {string} studentTestConceptResult.concept_desc name of concept
 * @return {object}
 */
function correctConceptAndProgram(studentTestConceptResult) {
  studentTestConceptResult.test_program_desc = correctTestProgramDesc(studentTestConceptResult.test_program_desc);
  studentTestConceptResult.concept_desc = correctConceptDesc(studentTestConceptResult.concept_desc);
  return studentTestConceptResult;
}

/**
 * extend each object with fields from PowerSchool, including the STUDENTS.ID and
 * STUDENTS.DCID fields based on STUDENTS.STATE_STUDENTNUMBER == ssid, and with the
 * TEST.ID field based on TEST.ID == test_name
 * @param  {array[object]} bufferedStudentTestConceptResults array of studentTestConceptResult objects
 * @return {array}                                           array of studentTestConceptResult + merged fields
 */
function mergeWithSsidAndTestName(bufferedStudentTestConceptResults) {
  const distinctSsids = uniqWith(bufferedStudentTestConceptResults, (a, b) => a.ssid === b.ssid)
    .map(item => item.ssid);
  const distinctTestNames = uniqWith(bufferedStudentTestConceptResults, (a, b) => a.test_program_desc === b.test_program_desc)
    .map(item => item.test_program_desc)
    .map(testProgramDesc => `EOL - ${testProgramDesc}`);

  return Observable.zip(
    getStudentIdsFromSsidBatchDual(distinctSsids),
    getTestIdsFromNamesBatch(distinctTestNames),

    (studentIds, testIds) => {
      const studentIdGroups = groupBy(item => parseInt(item.ssid), bufferedStudentTestConceptResults);
      const testNameGroups = groupBy(item => `EOL - ${item.test_program_desc}`, bufferedStudentTestConceptResults);

      mergeGroups(studentIdGroups, studentIds, item => parseInt(item.ssid));
      mergeGroups(testNameGroups, testIds, item => item.test_name);

      return flatten(testNameGroups);
    }
  );
}

/**
 * checks for duplicate student test records, and if any are found, they will have been filtered.
 * if no duplicate is found, create an object for coupler output
 * @param {object} studentTestConceptResult
 * @return {object}
 */
function checkForDuplicatesAndCreateFinalObject(studentTestConceptResult) {
  const fullSchoolYear = toFullSchoolYear(studentTestConceptResult.school_year);

  // TODO: create a batch service method that checks for student test score (concept)
  // there's going to be a lot of duplicates found here because i haven't found
  // a good way to filter out duplicates out when working with buffered items
  let matchingTestScore = studentTestScoreDuplicateCheck(
    studentTestConceptResult.student_number,
    fullSchoolYear,
    studentTestConceptResult.pct_of_questions_correct,
    studentTestConceptResult.test_id,
    studentTestConceptResult.concept_desc,
    studentTestConceptResult
  );

  return matchingTestScore.map(_ => {
    return {
      csvOutput: {
        'Test Date': studentTestConceptResult.test_date,
        'Student Id': studentTestConceptResult.student_id,
        'Student Number': studentTestConceptResult.student_number,
        'Grade Level': studentTestConceptResult.grade_level,
        'Concept Score Percent': studentTestConceptResult.pct_of_questions_correct
      },
      'extra': {
        testProgramDesc: studentTestConceptResult.test_program_desc,
        studentTestId: studentTestConceptResult.student_test_id
      }
    }
  });
}

export function testResultConceptTransform(observer) {
  return observer
    .map(correctConceptAndProgram)
    .map(createTestDate)
    .bufferCount(600)
    .flatMap(mergeWithSsidAndTestName)
    .flatMap(item => Observable.from(item))
    .flatMap(checkForDuplicatesAndCreateFinalObject);
}
