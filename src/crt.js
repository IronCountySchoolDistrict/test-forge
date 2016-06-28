require('babel-polyfill');

import { createWriteStream, truncateSync } from 'fs';
import Promise from 'bluebird';
import { Observable } from '@reactivex/rxjs';
import { isEmpty } from 'lodash';
import { gustav } from 'gustav';
import { merge, uniq, uniqWith, isEqual, truncate, extend, find } from 'lodash';

import {
  ssidToStudentNumber,
  testRecordToMatchingDcid,
  testNameToDcid,
  studentTestScoreDuplicateCheck,
  ssidsToStudentIds
}
from './blogic';
import {
  getStudentIdsFromSsidBatch,
  getTestIdsFromNamesBatch,
  getStudentIdsFromSsidBatchDual
} from './service';
import { logger } from './index';
import { printObj } from './util';
import { SamsCoupler } from './couplers/sams';
import { PowerSchoolCoupler } from './couplers/powerschool';
import { CsvCoupler } from './couplers/csv';

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

export function createWorkflow(prompt) {
  gustav.addCoupler(new SamsCoupler());
  gustav.addCoupler(new PowerSchoolCoupler());
  gustav.addCoupler(new CsvCoupler());

  let params = {};
  if (prompt.dest === 'Database') {
    params.coupler = 'powerschool';
    params.channel = prompt.table;
  } else if (prompt.dest === 'CSV') {
    params.coupler = 'csv';
    params.channel = {
      prompt: prompt
    };
    if (prompt.table === 'Test Results') {
      params.channel.groupByProperty = 'extra.testProgramDesc';
      params.channel.outputProperty = 'csvOutput';
    } else if (prompt.table === 'Test Result Concepts') {
      params.channel.groupByProperty = '';
      params.channel.outputProperty = '';
    } else if (prompt.table === 'U_StudentTestProficiency') {
      params.channel.groupByProperty = 'extra.testProgramDesc';
      params.channel.outputProperty = 'csvOutput';
    } else if (prompt.table === 'Test Scores') {
      params.channel.groupByProperty = 'test_program_desc';
      params.channel.outputProperty = ''; // use the entire object
    }
  }

  const workflow = gustav.createWorkflow()
    .from('sams', prompt.table)
    .transf(transformer, prompt)
    .to(...Object.keys(params).map(k => params[k]));

  return workflow;
}

function transformer(config, observer) {
  if (config.table === 'Test Results') {
    return testResultsTransform(observer);
  }
  if (config.table === 'Test Result Concepts') {
    return testResultConceptsTransform(observer);
  }
  if (config.table === 'U_StudentTestProficiency') {
    return proficiencyTransform(observer);
  }
  if (config.table === 'Test Scores') {
    return crtTestScoresTransform(observer);
  }
}

function testResultsTransform(observer) {

  return observer
    .map(item => {
      // If item.test_program_desc is spelled wrong, replace that value with the correctly spelled value
      item.test_program_desc = correctTestProgramDesc(item.test_program_desc);
      item.test_date = new Date(`05/01/${item.school_year}`);
      return item;
    })
    .bufferCount(500)
    .flatMap(bufferedItems => {
      let distinctSsids = bufferedItems.reduce((prev, curr) => {
        prev.push(curr.ssid);
        return prev;
      }, []);
      distinctSsids = uniq(distinctSsids);

      return Observable.zip(
        ssidsToStudentIds(distinctSsids),

        (studentIds) => {
          return bufferedItems
            .map(bufferedItem => {
              const matchingStudentIdItem = studentIds.filter(studentId => studentId.ssid === bufferedItem.ssid);
              if (matchingStudentIdItem.length) {
                bufferedItem.studentId = matchingStudentIdItem[0].studentId;
                bufferedItem.studentNumber = matchingStudentIdItem[0].studentNumber;
                return bufferedItem;
              } else {
                bufferedItem.studentId = null;
                bufferedItem.studentNumber = null;
                return bufferedItem;
              }
            })
            .filter(item => {
              return !!item.studentId || !!item.studentNumber;
            });
        }
      );
    })
    .flatMap(item => Observable.from(item))
    .flatMap(item => {
      return Observable.zip(
        testNameToDcid(item.test_program_desc, item),

        (matchingTest) => {
          return {
            matchingTestId: matchingTest,
            testResult: item
          };
        }
      );
    })
    .flatMap(item => {
      let fullSchoolYear = toFullSchoolYear(item.school_year);

      let matchingTestScore = studentTestScoreDuplicateCheck(
        item.testResult.studentNumber,
        fullSchoolYear,
        item.testResult.test_overall_score,
        item.matchingTestId,
        'Composite',
        item
      );

      return matchingTestScore
        .map(_ => ({
          csvOutput: {
            'Test Date': item.testResult.test_date,
            'Student Id': item.testResult.studentId,
            'Student Number': item.testResult.studentNumber,
            'Grade Level': item.testResult.grade_level,
            'Composite Score Num': item.testResult.test_overall_score
          },
          'extra': {
            testProgramDesc: item.testResult.test_program_desc,
            studentTestId: item.testResult.student_test_id
          }
        }));
    });
}

function proficiencyTransform(observer) {
  return observer
    .map(item => {
      // If test_program_desc is spelled wrong, replace that value with the correctly spelled value
      item.test_program_desc = item.test_program_desc === 'Earth Sytems Science' ? 'Earth Systems Science' : item.test_program_desc;
      if (item.test_program_desc === 'Algebra 1') {
        item.test_program_desc = 'Algebra I';
      }
      if (item.test_program_desc === 'Algebra 2') {
        item.test_program_desc = 'Algebra II';
      }
      return item;
    })
    .flatMap(item => {
      return Observable.zip(
        ssidToStudentNumber(item.ssid, item),
        testNameToDcid(item.test_program_desc, item),

        (studentNumber, matchingTest) => ({
          studentNumber: studentNumber,
          matchingTestId: matchingTest,
          testResult: item
        })
      );
    })
    .flatMap(item => {
      let fullSchoolYear = toFullSchoolYear(item.testResult.school_year);
      let matchingTestScore = testRecordToMatchingDcid(
        item.studentNumber,
        fullSchoolYear,
        item.testResult.test_overall_score,
        item.matchingTestId,
        item);

      return matchingTestScore
        .map(matchingTestScoreDcid => ({
          csvOutput: {
            studentTestScoreDcid: matchingTestScoreDcid,
            proficiency: item.testResult.proficiency
          },
          extra: {
            testProgramDesc: item.testResult.test_program_desc,
            studentTestId: item.testResult.student_test_id
          }
        }));
    });
}

function crtTestScoresTransform(observer) {
  return observer
    .map(item => {
      // Correct spelling and formatting inconsistencies
      item.test_program_desc = correctTestProgramDesc(item.test_program_desc);
      item.concept_desc = correctConceptDesc(item.concept_desc);
      return item;
    })
    .bufferCount(600)
    .flatMap(items => {
      const distinctItems = uniqWith(items, isEqual);
      const distinctTestNames = uniqWith(items, (a, b) => a.test_program_desc === b.test_program_desc)
        .map(item => item.test_program_desc)
        .map(testProgramDesc => `EOL - ${testProgramDesc}`);


      return Observable.zip(
        Observable.fromPromise(getTestIdsFromNamesBatch(distinctTestNames)),

        testIds => {
          return distinctItems.map(item => {
            let matchingTestId = testIds.rows.filter(testId => testId.TEST_NAME === `EOL - ${item.test_program_desc}`);
            if (matchingTestId.length) {
              item.testId = matchingTestId[0].ID;
            }
            item.scoreName = truncate(item.concept_desc, {
              length: 35,
              separator: ' '
            });
            return item;
          });
        }
      );
    })
    .flatMap(items => Observable.from(items));
}

function testResultConceptsTransform(observer) {
  return observer
    .map(studentTestConceptResult => {
      studentTestConceptResult.test_program_desc = correctTestProgramDesc(studentTestConceptResult.test_program_desc);
      studentTestConceptResult.concept_desc = correctConceptDesc(studentTestConceptResult.concept_desc);
      studentTestConceptResult.test_date = new Date(`05/01/${studentTestConceptResult.school_year}`);
      return studentTestConceptResult;
    })
    .bufferCount(600)
    .flatMap(bufferedStudentTestConceptResults => {
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
    })
    .flatMap(item => Observable.from(item))
    .flatMap(testConceptResult => {
      const fullSchoolYear = toFullSchoolYear(testConceptResult.school_year);

      // create a batch service method that checks for student test score (concept)
      // there's going to be a lot of duplicates found here because i haven't found
      // a good way to filter out duplicates out when working with buffered items
      let matchingTestScore = studentTestScoreDuplicateCheck(
        testConceptResult.student_number,
        fullSchoolYear,
        testConceptResult.pct_of_questions_correct,
        testConceptResult.test_id,
        testConceptResult.concept_desc,
        testConceptResult
      );

      return matchingTestScore.map(_ => {
        console.log(testConceptResult);
        return {
          csvOutput: {
            'Test Date': testConceptResult.test_date,
            'Student Id': testConceptResult.student_id,
            'Student Number': testConceptResult.student_number,
            'Grade Level': testConceptResult.grade_level,
            'Concept Score Percent': testConceptResult.pct_of_questions_correct
          },
          'extra': {
            testProgramDesc: testConceptResult.test_program_desc,
            studentTestId: testConceptResult.student_test_id
          }
        }
      })
    })

}

function correctConceptDesc(conceptDesc) {
  // remove roman numerals
  conceptDesc = conceptDesc.replace(/^(X{0,3}\s+|IX\s+|IV\s+|V?I{0,3}\s*)/g, '');

  // remove all dots
  conceptDesc = conceptDesc.replace(/(\.)+$/g, '');

  // remove all numbers at the beginning of concept_desc
  conceptDesc = conceptDesc.replace(/^([0-9]+)\s+/g, '');

  // remove all asterisks
  conceptDesc = conceptDesc.replace(/(\*)+/ig, '');

  // spelling corrections
  conceptDesc = conceptDesc.replace(/(Comprhnsn)/ig, 'Comprehension');
  conceptDesc = conceptDesc.replace(/(\band\b)/ig, '&');
  conceptDesc = conceptDesc.replace(/(intgrted)/ig, 'integrated');
  conceptDesc = conceptDesc.replace(/(Cycle As It Orbits Earth)/ig, 'Cycle As It Orbits The Earth');
  conceptDesc = conceptDesc.replace(/(\bAmts\b)/ig, 'Amounts');
  conceptDesc = conceptDesc.replace(/(Forms, But)/ig, 'Forms &');
  conceptDesc = conceptDesc.replace(/(\bSpatial, )/ig, 'Spacial ');
  conceptDesc = conceptDesc.replace(/(\bSituatio\b)/ig, 'Situations');
  conceptDesc = conceptDesc.replace(/(Hydrosphere, Affects)/ig, 'Hydrosphere Affects');
  conceptDesc = conceptDesc.replace(/(Environment & Humans Impact On)/ig, 'Environment & Human Impact On');
  conceptDesc = conceptDesc.replace(/(Uplife,)/ig, 'Uplift,');
  conceptDesc = conceptDesc.replace(/(Concepts From Statistics & Apply)/ig, 'Concepts From Probability & Statistics & Apply');
  conceptDesc = conceptDesc.replace(/(Reasonable Conlusions)/ig, 'Reasonable Conclusions');
  conceptDesc = conceptDesc.replace(/\b(\/)/g, ' /');
  conceptDesc = conceptDesc.replace(/(\/)\b/g, '/ ');
  conceptDesc = conceptDesc.replace(/(Force, Mass, &)/ig, 'Force, Mass &');
  conceptDesc = conceptDesc.replace(/(Acclerations)/ig, 'Acceleration');
  conceptDesc = conceptDesc.replace(/(Accelerations)/ig, 'Acceleration');
  conceptDesc = conceptDesc.replace(/(& Application Of Waves)/ig, '& Applications Of Waves');
  conceptDesc = conceptDesc.replace(/(Enviornment)/ig, 'Environment');
  conceptDesc = conceptDesc.replace(/(Environmnet)/ig, 'Environment');
  conceptDesc = conceptDesc.replace(/(Force, & Motion)/ig, 'Force & Motion');
  conceptDesc = conceptDesc.replace(/(, & )/ig, ' & ');
  conceptDesc = conceptDesc.replace(/(\bOrganismsof\b)/ig, 'Organism of');
  conceptDesc = conceptDesc.replace(/\b(W \/)/ig, 'With');
  conceptDesc = conceptDesc.replace(/(Language \/ Operations)/ig, 'Language & Operations');
  conceptDesc = conceptDesc.replace(/(& Common Organism Of)/ig, '& Common Organisms Of');
  conceptDesc = conceptDesc.replace(/(Organisms Of Utah Environments)/ig, 'Organisms Of Utah\'s Environment');
  conceptDesc = conceptDesc.replace(/(Interact With On Another)/ig, 'Interact With One Another');
  conceptDesc = conceptDesc.replace(/(With & Is Altered By Earth Layers|With & Is Altered By Earth\'s Layers)/ig, 'With & Is Altered By The Earth\'s Layers');
  conceptDesc = conceptDesc.replace(/(Affect Living Systems, Earth Is Unique)/ig, 'Affect Living Systems & Earth Is Unique');
  conceptDesc = conceptDesc.replace(/(Axiz)/ig, 'Axis');
  conceptDesc = conceptDesc.replace(/(Earth\'s Revolving Environment Affect)/ig, 'Earth\'s Evolving Environment Affect');
  conceptDesc = conceptDesc.replace(/(Earth\'s Plates & Causes Plates)/ig, 'Earth\'s Plates & Causes The Plates');
  conceptDesc = conceptDesc.replace(/(As It Revolves Aroung The Sun)/ig, 'As It Revolves Around The Sun');
  conceptDesc = conceptDesc.replace(/(Offsping)/ig, 'Offspring');
  conceptDesc = conceptDesc.replace(/(Cells That Have Structures)/ig, 'Cells That Have Structure');
  conceptDesc = conceptDesc.replace(/(Heat Light & Sound)/ig, 'Heat, Light & Sound');
  conceptDesc = conceptDesc.replace(/(Nature Of Changes In Matter)/ig, 'Nature Of Change In Matter');
  conceptDesc = conceptDesc.replace(/(Relationshi)/ig, 'Relationships');
  conceptDesc = conceptDesc.replace(/(Effetively)/ig, 'Effectively');
  conceptDesc = conceptDesc.replace(/(Diveristy)/ig, 'Diversity');
  conceptDesc = conceptDesc.replace(/(Relationshipsp)/ig, 'Relationship');
  conceptDesc = conceptDesc.replace(/(Sciene)/ig, 'Science');
  conceptDesc = conceptDesc.replace(/(Demostrate)/ig, 'Demonstrate');
  conceptDesc = conceptDesc.replace(/(Relationshipsps)/ig, 'Relationships');
  conceptDesc = conceptDesc.replace(/(Albegra)/ig, 'Algebra');
  conceptDesc = conceptDesc.replace(/(Nterpret)/ig, 'Interpret');
  conceptDesc = conceptDesc.replace(/(Ocabulary)/ig, 'Vocabulary');
  conceptDesc = conceptDesc.replace(/(Vvocabulary)/ig, 'Vocabulary');
  conceptDesc = conceptDesc.replace(/(Determine Area & Surface Area Polygons)/ig, 'Determine Area Of Polygons & Surface Area');
  conceptDesc = conceptDesc.replace(/(Factors Determining Strength Of Gravitational)/ig, 'Factors Determining The Strength Of Gravitational');
  conceptDesc = conceptDesc.replace(/(Iinterpret)/ig, 'Interpret');
  conceptDesc = conceptDesc.replace(/(Offspring Inherit Traits That Affect Survival In The Environment)/ig, 'Offspring Inherit Traits That Make Them More Or Less Suitable To Survive In The Environment');
  conceptDesc = conceptDesc.replace(/(Offspring Inherit Traits That Affect Survival In The Environment)/ig, 'Offspring Inherit Traits That Make Them More Or Less Suitable To Survive In The Environment');
  conceptDesc = conceptDesc.replace(/(Organisms Are Composed Of 1 Or More Cells That Are Made Of Molecules...& Perform Life Functions)/ig, 'Organisms Are Composed Of One Or More Cells That Are Made Of Molecules & Perform Life Functions');
  conceptDesc = conceptDesc.replace(/(Organisms Are Composed Of One Or More Cells That Are Made)/ig, 'Organisms Are Composed Of One Or More Cells That Are Made Of Molecules & Perform Life Functions');
  conceptDesc = conceptDesc.replace(/(Organs In An Organism Are Made Of Cells That Perform Life Functions)/ig, 'Organs In An Organism Are Made Of Cells That Have Structure & Perform Specific Life Functions');
  conceptDesc = conceptDesc.replace(/^(Properties & Behavior Of Heat, Light & Sound)$/ig, 'Understand Properties & Behavior Of Heat, Light & Sound');
  conceptDesc = conceptDesc.replace(/^(Awareness Of Social & Historical Aspects Of Science)$/ig, 'Demonstrate Awareness Of Social & Historical Aspects Of Science');

  // Convert to case where all words start with capital letter
  conceptDesc = conceptDesc.replace(/\w\S*/g, txt => txt.charAt(0).toUpperCase() + txt.substr(1).toLowerCase());

  return conceptDesc;
}

function groupBy(keyExtract, arr) {
  return arr.reduce((result, item) => {
    const key = keyExtract(item);
    if (result.has(key)) {
      result.set(key, [...result.get(key), item]);
    } else {
      result.set(key, [item]);
    }

    return result;
  }, new Map());
}

function mergeGroups(groups, array, mergeKeyExtract) {
  try {
    array.forEach(it => {
      try {
        const mergeKey = mergeKeyExtract(it);
        const group = groups.get(mergeKey);
        if (!group) {
          throw `${mergeKey} not found`;
        }
        group.map(item => {
          return Object.assign(item, it);
        });

      } catch (e) {
        console.log(it, e);
      }
    });
  } catch(e) {
    console.log('merge error');
    console.log(e);
    console.log(array);
  }

}

function flatten(groups, arrayMap) {
	let arr = []
	for (const [index, group] of groups.entries()) {
  	arr.push(...group);
  }
  return arr;
}

function correctTestProgramDesc(test_program_desc) {
  test_program_desc = test_program_desc === 'Earth Sytems Science' ? 'Earth Systems Science' : test_program_desc;
  test_program_desc = test_program_desc === 'Algebra 1' ? 'Algebra I' : test_program_desc;
  test_program_desc = test_program_desc === 'Algebra 2' ? 'Algebra II' : test_program_desc;
  return test_program_desc;
}

/**
 * converts a school year in the format "2011" to "2010-2011"
 * @param  {string|number} shortSchoolYear
 * @return {string}
 */
function toFullSchoolYear(shortSchoolYear) {
  return `${shortSchoolYear - 1}-${shortSchoolYear}`;
}

function consoleNode(observable) {
  return observable.subscribe(x => console.dir(x));
}
