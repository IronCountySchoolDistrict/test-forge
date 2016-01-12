require('babel-polyfill');


import {
  createWriteStream,
  truncateSync
}
from 'fs';
import Promise from 'bluebird';
import fs from 'fs-promise';
import {
  Observable
}
from '@reactivex/rxjs';

import {
  isEmpty
}
from 'lodash';
import {
  gustav
}
from 'gustav';

import json2csv from 'json2csv';
import {
  EOL
}
from 'os';

import {
  getStudentIdFromSsid,
  getStudentNumberFromSsid,
  getTestDcid,
  getMatchingStudentTestScore,
  getMatchingTests
}
from './service';

import {
  logger
}
from './index';

import {
  printObj
}
from './util';

var toCSV = Promise.promisify(json2csv);

var count = 0;
var blankCount = 0;

async function asyncExec(command) {
  return new Promise((resolve, reject) => {
    exec(command, function(error, stdout, stderr) {
      if (error) {
        reject(error);
      } else if (stderr) {
        reject(stderr);
      } else {
        resolve(stdout);
      }
    });
  });
}

export function createWorkflow(sourceObservable, prompt) {
  var config = {
    prompt: prompt
  };

  gustav.source('dataSource', () => sourceObservable);

  return gustav.createWorkflow()
    .source('dataSource')
    .transf(transformer, config)
    /*.transf(filterEmpty)*/
    .sink(crtCsvSink, config);
  // .sink(consoleNode);
}

function testResultsTransform(observer) {
  return observer
    .flatMap(item => {
      // TODO: refactor this so it builds an object with nice helpful key names
      let asyncProps = [getStudentNumberFromSsid(item.ssid), getStudentIdFromSsid(item.ssid)];
      return Observable.zip(

        Observable.fromPromise(Promise.all(asyncProps))
        .catch(e => {
          logger.log('info', `Error fetching student fields for ssid: ${item.ssid}`, {
            psDbError: printObj(e)
          });
          logger.log('info', `SAMS DB Record for student_test_id: ${item.student_test_id}`, {
            sourceData: printObj(item)
          });
          return Observable.of({});
        }),

        Observable.of(item),

        (s1, s2) => ({
          asyncProps: s1,
          testResult: s2
        })
      );
    })
    .filter(item => {
      return !isEmpty(item.asyncProps);
    })
    .flatMap(item => {
      let testName = toTestName(item.testResult.test_program_desc);

      // Get the TEST.ID from PowerSchool that matches the test_program_desc
      let matchingTest = getMatchingTests(testName)
        .then(r => {
          if (!r.rows.length === 1) {
            throw {
              studentTestScore: r,
              testResult: item.testResult,
              message: `expected getMatchingTests to return 1 record, got ${r.rows.length} rows`
            };
          } else {
            return r.rows[0].ID
          }
        });

      return Observable.zip(
        Observable.of(item),

        Observable.fromPromise(matchingTest)
        .catch(e => {
          logger.log('info', `Error finding matching test for ssid: ${item.testResult.ssid}`, {
            psDbError: printObj(e)
          });
          logger.log('info', `SAMS DB Record for student_test_id: ${item.testResult.student_test_id}`, {
            sourceData: printObj(item)
          });
          return Observable.of(0);
        }),

        (itemSrc, matchingTest) => {
          return {
            testResult: itemSrc.testResult,
            asyncProps: itemSrc.asyncProps,
            matchingTest: matchingTest
          };
        }
      );
    })
    .filter(item => {
      // item.matchingTest should NOT be false (0) for the item to be allowed through
      return !!item.matchingTest;
    })
    .flatMap(item => {
      let fullSchoolYear = toFullSchoolYear(item.testResult.school_year);
      let matchingTestscore = getMatchingStudentTestScore(
          item.asyncProps[0], // student number
          fullSchoolYear,
          item.testResult.test_overall_score,
          item.matchingTest)
        .then(r => {
          // Expecting there to NOT be any matching student test score record,
          // so if there is one or more, throw an exception
          if (r.rows.length) {
            throw {
              studentTestScore: r,
              testResult: item.testResult,
              message: `expected getMatchingStudentTestScore to return 0 records, got ${r.rows.length} rows`
            };
          } else {
            return {};
          }
        });

      let matchingTestScoreObservable = Observable.fromPromise(matchingTestscore).catch(e => {
        logger.log('info', `Error checking for matching student test records for ssid: ${item.ssid}`, {
          psDbError: printObj(e)
        });
        //TODO: refactor this to use null instead of object
        return Observable.of({});
      });

      return Observable.zip(
        matchingTestScoreObservable,
        Observable.of(item),

        (s1, s2) => {
          return {
            matchingStudentTestScore: s1,
            testResult: s2.testResult,
            asyncProps: s2.asyncProps
          };
        }
      );
    })
    .filter(item => {
      // item.matchingStudentTestScore should be blank (no duplicate score in the database)
      return isEmpty(item.matchingStudentTestScore);
    })
    .map(item => {
      if (!isEmpty(item.asyncProps)) {
        return {
          'csvOutput': {
            'Test Date': item.testResult.school_year,
            'Student Id': item.asyncProps[0],
            'Student Number': item.asyncProps[1],
            'Grade Level': item.testResult.grade_level,
            'Composite Score Alpha': item.testResult.test_overall_score
          },
          'extra': {
            testProgramDesc: item.testResult.test_program_desc,
            studentTestId: item.testResult.student_test_id
          }
        };
      }
    });
}

// TODO: refactor these flatMaps to combine all independent
// promises to be settled within the same flatMap
function proficiencyTransform(observer) {
  return observer
    .flatMap(item => {
      // TODO: refactor this so it builds an object with nice helpful key names
      let asyncProps = [getStudentNumberFromSsid(item.ssid), getStudentIdFromSsid(item.ssid)];
      return Observable.zip(

        Observable.fromPromise(Promise.all(asyncProps))
        .catch(e => {
          logger.log('info', `Error fetching student fields for ssid: ${item.ssid}`, {
            psDbError: printObj(e)
          });
          logger.log('info', `SAMS DB Record for student_test_id: ${item.student_test_id}`, {
            sourceData: printObj(item)
          });
          return Observable.of({});
        }),

        Observable.of(item),

        (s1, s2) => ({
          asyncProps: s1,
          proficiency: s2
        })
      );
    })
    .filter(item => {
      console.log('item == %j', item);
      return !isEmpty(item.asyncProps);
    })
    .flatMap(item => {
      let testName = toTestName(item.proficiency.test_program_desc);

      // Get the TEST.ID from PowerSchool that matches the test_program_desc
      let matchingTest = getMatchingTests(testName)
        .then(r => {
          if (!r.rows.length === 1) {
            throw {
              studentTestScore: r,
              proficiency: item.proficiency,
              message: `expected getMatchingTests to return 1 record, got ${r.rows.length} rows`
            };
          } else {
            return r.rows[0].ID
          }
        });

      return Observable.zip(
        Observable.of(item),

        Observable.fromPromise(matchingTest)
        .catch(e => {
          logger.log('info', `Error finding matching test for ssid: ${item.proficiency.ssid}`, {
            psDbError: printObj(e)
          });
          logger.log('info', `SAMS DB Record for student_test_id: ${item.proficiency.student_test_id}`, {
            sourceData: printObj(item)
          });
          return Observable.of(0);
        }),

        (itemSrc, matchingTest) => {
          return {
            proficiency: itemSrc.proficiency,
            asyncProps: itemSrc.asyncProps,
            matchingTest: matchingTest
          };
        }
      );
    })
    .filter(item => {
      console.log('item == %j', item);
      // item.matchingTest should NOT be false (0) for the item to be allowed through
      return !!item.matchingTest;
    })
    .flatMap(item => {
      let fullSchoolYear = toFullSchoolYear(item.proficiency.school_year);
      let matchingTestscore = getMatchingStudentTestScore(
          item.asyncProps[0], // student number
          fullSchoolYear,
          item.proficiency.test_overall_score,
          item.matchingTest)
        .then(r => {
          // Expecting there to NOT be any matching student test score record,
          // so if there is one or more, throw an exception
          if (!r.rows.length) {
            throw {
              studentTestScore: r,
              testResult: item.testResult,
              message: `expected getMatchingStudentTestScore to return 1 records, got ${r.rows.length} rows`
            };
          } else {
            return r.rows;
          }
        });

      let matchingTestScoreObservable = Observable.fromPromise(matchingTestscore).catch(e => {
        logger.log('info', `Error searching for matching student test records for ssid: ${item.proficiency.ssid}`, {
          psDbError: printObj(e)
        });
        //TODO: refactor this to use null instead of object
        return Observable.of({});
      });

      return Observable.zip(
        matchingTestScoreObservable,
        Observable.of(item),

        (s1, s2) => {
          return {
            studentTestScore: s1,
            proficiency: s2.proficiency,
            asyncProps: s2.asyncProps
          };
        }
      );
    })
    .filter(item => {
      console.log('filter item == %j', item);
      // item.matchingStudentTestScore should be blank (no duplicate score in the database)
      return !isEmpty(item.studentTestScore);
    })
    .map(item => {
      console.log('item == %j', item);
      if (!isEmpty(item.asyncProps)) {
        return {
          csvOutput: {
            studentTestScoreDcid: item.studentTestScore,
            benchmark: item.testResult.proficiency
          },
          extra: {
            testProgramDesc: item.testResult.test_program_desc,
            studentTestId: item.testResult.student_test_id
          }
        };
      }
    });
}

function transformer(config, observer) {
  if (config.prompt.table === 'Test Results') {
    return testResultsTransform(observer);
  }
  if (config.prompt.table === 'U_StudentTestProficiency') {
    return proficiencyTransform(observer);
  }
}

function filterEmpty(observer) {
  return observer
    .filter(item => {
      if (!(!isEmpty(item.testResult) && !isEmpty(item.extra))) {
        blankCount++;
      }
      return !isEmpty(item.testResult) && !isEmpty(item.extra);
    });
}


/**
 * converts a school year in the format "2011" to "2010-2011"
 * @param  {string|number} shortSchoolYear
 * @return {string}
 */
function toFullSchoolYear(shortSchoolYear) {
  return `${shortSchoolYear - 1}-${shortSchoolYear}`;
}

/**
 * converts a test_program_desc value to a PS.Test.name value
 * @param  {string} testProgramDesc PS.Test.name
 * @return {string}                 output file name
 */
function toTestName(testProgramDesc) {
  if (testProgramDesc.indexOf('Grade Language Arts') !== -1) {
    return 'EOL - Language Arts';
  } else if (testProgramDesc.indexOf('Grade Science') !== -1) {
    return 'EOL - Science';
  } else if (testProgramDesc.indexOf('Grade Math') !== -1) {
    return 'EOL - Math';
  } else if (testProgramDesc.indexOf('Algebra I') !== -1) {
    return 'EOL - Algebra I';
  } else if (testProgramDesc.indexOf('Biology') !== -1) {
    return 'EOL - Biology';
  } else if (testProgramDesc.indexOf('UAA Math') !== -1) {
    return 'EOL - UAA Math';
  } else if (testProgramDesc.indexOf('UAA Language') !== -1) {
    return 'EOL - UAA Language';
  } else if (testProgramDesc.indexOf('UAA Science') !== -1) {
    return 'EOL - UAA Science';
  } else if (testProgramDesc.indexOf('Earth Systems Science') !== -1 ||
    testProgramDesc.indexOf('Earth Sytems Science') !== -1) {
    return 'EOL - Earth Systems Science';
  } else if (testProgramDesc.indexOf('6th Grade Math Common Core') !== -1) {
    return 'EOL - Math Common Core';
  } else if (testProgramDesc.indexOf('6th Grade Math Existing Core') !== -1) {
    return 'EOL - Math Existing Core';
  } else if (testProgramDesc.indexOf('Direct Writing I') !== -1) {
    return 'EOL - Direct Writing I';
  } else if (testProgramDesc.indexOf('Direct Writing II') !== -1) {
    return 'EOL - Direct Writing II';
  } else if (testProgramDesc.indexOf('Direct Writing') !== -1) {
    return 'EOL - Direct Writing';
  } else if (testProgramDesc.indexOf('Pre-Algebra') !== -1) {
    return 'EOL - Pre-Algebra';
  } else if (testProgramDesc.indexOf('Algebra I') !== -1) {
    return 'EOL - Algebra I';
  } else if (testProgramDesc.indexOf('Algebra II') !== -1) {
    return 'EOL - Algebra II';
  } else if (testProgramDesc.indexOf('Geometry') !== -1) {
    return 'EOL - Geometry';
  } else if (testProgramDesc.indexOf('Chemistry') !== -1) {
    return 'EOL - Chemistry';
  } else if (testProgramDesc.indexOf('Elementary Algebra') !== -1) {
    return 'EOL - Elementary Algebra';
  } else if (testProgramDesc.indexOf('Physics') !== -1) {
    return 'EOL - Physics';
  } else {
    return 'other';
  }
}

function consoleNode(observer) {
  observer.subscribe(x => {
    console.log('in consoleNode');
    console.dir(x);
    console.log(`x == ${x}`);
  });
}

function crtCsvSink(config, observable) {
  return observable
    .groupBy(x => toTestName(x.extra.testProgramDesc))
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
  console.log('config == %j', config);
  return srcObservable.concatMap(function(item, i) {
    if (i === 0) {
      console.log('getting outputFilename');
      let outputFilename = `output/${toTestName(item.extra.testProgramDesc)}-${config.prompt.table}.txt`;

      // creates file if it doesn't exist
      var ws = createWriteStream(outputFilename, {
        flags: 'a'
      });
      console.log(`created write stream for ${outputFilename}`);

      try {
        ws.on('open', function(fd) {
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
