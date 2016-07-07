import { truncate, uniqWith, isEqual } from 'lodash';
import { Observable } from '@reactivex/rxjs';

import { correctTestProgramDesc, correctConceptDesc } from './util';
import { getTestIdsFromNamesBatch } from '../../service';

/**
 * fix test_program_desc and concept_desc spelling errors and inconsistencies
 * @param {object} studentTestResult row of crt data with student test concepts results
 * @param {string} studentTestResult.test_program_desc name of test
 * @param {string} studentTestResult.concept_desc name of concept
 * @return {object}
 */
function correctConceptAndProgram(testScore) {
  testScore.test_program_desc = correctTestProgramDesc(testScore.test_program_desc);
  testScore.concept_desc = correctConceptDesc(testScore.concept_desc);
  return testScore;
}

/**
 * merges test IDs with test names and creates the final test score object
 * @param {array[object]} testScores
 * @return {observable}
 */
function mergeTestIdAndCreateFinalObject(testScores) {
  const distinctTestScores = uniqWith(testScores, isEqual);
  const distinctTestNames = uniqWith(testScores, (a, b) => a.test_program_desc === b.test_program_desc)
    .map(item => item.test_program_desc)
    .map(testProgramDesc => `EOL - ${testProgramDesc}`);

  return Observable.zip(
    Observable.fromPromise(getTestIdsFromNamesBatch(distinctTestNames)),

    testIds => {
      return distinctTestScores.map(item => {
        let matchingTestId = testIds.filter(testId => testId.test_name === `EOL - ${item.test_program_desc}`);
        if (matchingTestId.length) {
          item.testId = matchingTestId[0].test_id;
        }
        item.scoreName = truncate(item.concept_desc, {
          length: 35,
          separator: ' '
        });

        return item;
      });
    }
  );
}

export function testScoreTransform(observer) {
  return observer
    .map(correctConceptAndProgram)
    .bufferCount(600)
    .flatMap(mergeTestIdAndCreateFinalObject)
    .flatMap(items => Observable.from(items));
}
