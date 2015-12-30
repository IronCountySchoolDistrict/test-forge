require('babel-polyfill');

import {
  getStudentIdFromSsid,
  getStudentNumberFromSsid,
  getTestDcid,
  getMatchingStudentTestScore
}
from './service';

import Promise from 'bluebird';
import fs from 'fs-promise';
import {
  appendFileSync, createWriteStream, write
}
from 'fs';
import {
  Observable
}
from '@reactivex/rxjs';
import {
  asyncPrependFile
}
from './workflow';
import {
  isEmpty
}
from 'lodash';
import {
  gustav
}
from 'gustav';
import {
  EOL
}
from 'os';
import json2csv from 'json2csv';
import util from 'util';
import {
  logger
}
from './index';
import winston from 'winston';

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

export class CRTTestResults {
  constructor(record) {
    this.ssid = record.ssid;
    this.schoolYear = record.school_year;
    this.gradeLevel = record.grade_level;
    this.compositeScore = record.test_overall_score;
    this.testProgramDesc = record.test_program_desc;
  }
}

export function createWorkflow(sourceObservable) {
  gustav.source('dataSource', () => sourceObservable);

  return gustav.createWorkflow()
    .source('dataSource')
    .transf(transformer)
    .transf(filterEmpty)
    .sink(crtCsvSink);
  // .sink(consoleNode);
}

function testResultsTransform(observer) {
  return observer
    .flatMap(item => {
      let asyncProps = [getStudentNumberFromSsid(item.ssid), getStudentIdFromSsid(item.ssid)];
      return Observable.zip(
        Observable.fromPromise(
          Promise.all(asyncProps)
        )
        .catch(e => {
          logger.log('info', `Error fetching student fields for ssid: ${item.ssid}`, {
            psDbError: util.inspect(e, {
              showHidden: false,
              depth: null
            })
          });
          logger.log('info', `SAMS DB Record for student_test_id: ${item.student_test_id}`, {
            sourceData: util.inspect(item, {
              showHidden: false,
              depth: null
            })
          });
          return Observable.of({});
        }),
        Observable.of(item),
        function(s1, s2) {
          return {
            asyncProps: s1,
            testResults: s2
          };
        }
      );
    })
    .map(item => {
      if (!isEmpty(item.asyncProps)) {
        return {
          'testResults': {
            'Test Date': item.testResults.school_year,
            'Student Id': item.asyncProps[0],
            'Student Number': item.asyncProps[1],
            'Grade Level': item.testResults.grade_level,
            'Composite Score Alpha': item.testResults.test_overall_score
          },
          'extra': {
            testProgramDesc: item.testResults.test_program_desc,
            studentTestId: item.testResults.student_test_id
          }
        };
      } else {
        return item;
      }
    });
}

function transformer(observer) {
  return testResultsTransform(observer);
}

function filterEmpty(observer) {
  return observer
    .filter(item => {
      if (!(!isEmpty(item.testResults) && !isEmpty(item.extra))) {
        blankCount++;
      }
      let empty = !isEmpty(item.testResults) && !isEmpty(item.extra);
      return empty;
    });
}

/**
 * converts a test_program_desc value to a PS.Test.name value
 * @param  {[type]} testProgramDesc [description]
 * @return {string}                 output file name
 */
function toFileName(testProgramDesc) {
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

function crtCsvSink(observer) {
  return observer
    .groupBy(x => toFileName(x.extra.testProgramDesc))
    .subscribe(obs => {
      let ws;
      let outputFilename;
      let csvGroupObs = obs.concatMap((item, i) => {
        if (i === 0) {
          outputFilename = `output/${toFileName(item.extra.testProgramDesc)}.txt`;
          ws = createWriteStream(outputFilename, {
            flags: 'a'
          });
          try {
            fs.truncateSync(outputFilename);
            return toCSV({
                data: item.testResults,
                del: '\t',
                hasCSVColumnTitle: true
              })
              .then(csvStr => {
                return Observable.of(csvStr.replace(/"/g, ''));
              });
          } catch (e) {
            return toCSV({
                data: item.testResults,
                del: '\t',
                hasCSVColumnTitle: true
              })
              .then(csvStr => {
                return Observable.of(csvStr.replace(/"/g, ''));
              });
          }
        } else {
          return toCSV({
              data: item.testResults,
              del: '\t',
              hasCSVColumnTitle: false
            })
            .then(csvStr => {
              return Observable.of(EOL + csvStr.replace(/"/g, ''));
            });
        }
      });

      csvGroupObs.subscribe(csvStr => {
        ws.write(csvStr.value);
      });

    });
}
