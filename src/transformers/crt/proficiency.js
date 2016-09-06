import { Observable } from '@reactivex/rxjs';
import { correctTestProgramDesc } from './util';


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

function mergeSsidandTest(testResult) {
  return Observable.zip(
    ssidToStudentNumber(testResult.ssid, testResult),
    testNameToDcid(testResult.test_program_desc, testResult),

    (studentNumber, matchingTest) => ({
      studentNumber: studentNumber,
      matchingTestId: matchingTest,
      testResult: testResult
    })
  );
}

function checkForDuplicatesAndCreateFinalObject(testResult) {
  let fullSchoolYear = toFullSchoolYear(testResult.testResult.school_year);
  let matchingTestScore = testRecordToMatchingDcid(
    testResult.studentNumber,
    fullSchoolYear,
    testResult.testResult.test_overall_score,
    testResult.matchingTestId,
    item);

  return matchingTestScore.map(_ => ({
    csvOutput: {
      studentTestScoreDcid: matchingTestScoreDcid,
      proficiency: testResult.testResult.proficiency
    },
    extra: {
      testProgramDesc: testResult.testResult.test_program_desc,
      studentTestId: testResult.testResult.student_test_id
    }
  }));
}

export function proficiencyTransform(observable) {
  return observable
    .map(correctTestProgramDesc)
    .flatMap(mergeSsidandTest)
    .flatMap(checkForDuplicatesAndCreateFinalObject);
}
