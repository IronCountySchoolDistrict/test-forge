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

var toCSV = Promise.promisify(json2csv);

export class CRTTestResults {
  constructor(record) {
    this.ssid = record.ssid;
    this.schoolYear = record.school_year;
    this.gradeLevel = record.grade_level;
    this.compositeScore = record.test_overall_score;
    this.testProgramDesc = record.test_program_desc;
  }

  get studentId() {
    try {
      return getStudentIdFromSsid(this.ssid)
        .then(r => {
          return new Promise((resolve, reject) => {
            console.log('settled Promise for studentId()');
            if (r.rows.length > 1) {
              console.dir(r);
              console.dir(this);
              reject(new Error('Expected getStudentId() to return one row, got back more than one record'));
            } else {
              try {
                resolve(r.rows[0].ID);
              } catch (e) {
                console.dir(r);
                console.dir(this);
              }
            }
          });
        });

    } catch (e) {
      console.error(e.trace);
    }
  }

  get studentNumber() {
    try {
      return getStudentNumberFromSsid(this.ssid)
        .then(r => {
          return new Promise((resolve, reject) => {
            console.log('settled Promise for studentNumber()');
            if (r.rows.length > 1) {
              console.dir(r);
              console.dir(this);
              reject(new Error('Expected getStudentDcidFromSsid() to return one row, got back more than one record'));
            } else {
              try {
                resolve(r.rows[0].STUDENT_NUMBER);
              } catch (e) {
                console.error(e.trace);
                console.dir(r);
                console.dir(this);
              }
            }
          });
        })

    } catch (e) {
      console.error(e.trace);
    }
  }

}

export function createWorkflow(sourceObservable) {
  gustav.source('dataSource', () => sourceObservable);

  return gustav.createWorkflow()
    .source('dataSource')
    .transf(transformer)
    .sink(crtCsvSink);
}

function testResultsTransform(observer) {

  return observer
    .flatMap(item => {
      let test = new CRTTestResults(item);
      return Observable.zip(
        Observable.fromPromise(Promise.all([test.studentId, test.studentNumber])),
        Observable.of(test),
        function(s1, s2) {
          return {
            asyncProps: s1,
            testResults: s2
          }
        }
      ).catch(e => {console.log('in zip catch'); console.dir(e);});
    })
    .map(item => {
      return {
        'testResults': {
          'Test Date': item.testResults.schoolYear,
          'Student Id': item.asyncProps[0],
          'Student Number': item.asyncProps[1],
          'Grade Level': item.testResults.gradeLevel,
          'Composite Score Alpha': item.testResults.compositeScore
        },
        'extra': {
          testProgramDesc: item.testResults.testProgramDesc
        }
      };
    });
}

function transformer(observer) {
  return testResultsTransform(observer);
}

/**
 * converts a test_program_desc value to a PS.Test.name value
 * @param  {[type]} testProgramDesc [description]
 * @return {[type]}                 [description]
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
    return 'UAA Math';
  } else if (testProgramDesc.indexOf('UAA Language') !== -1) {
    return 'UAA Language';
  } else if (testProgramDesc.indexOf('UAA Science') !== -1) {
    return 'UAA Science';
  } else if (testProgramDesc.indexOf('Earth Systems Science') !== -1 ||
    testProgramDesc.indexOf('Earth Sytems Science') !== -1) {
    return 'EOL - Earth Systems Science';
  } else if (testProgramDesc.indexOf('6th Grade Math Common Core')) {
    return 'EOL - Math Common Core';
  } else if (testProgramDesc.indexOf('6th Grade Math Existing Core')) {
    return 'EOL - Math Existing Core';
  }
}

function consoleNode(observer) {
  observer.subscribe(x => console.log(x));
}

function crtCsvSink(observer) {
  observer
    .groupBy(
      x => toFileName(x.extra.testProgramDesc),
      x => x
    )
    .subscribe(obs => {
      obs.first().subscribe(async function(item) {
        console.log('in first');
        console.log(item);
        let outputFilename = `output/${toFileName(item.extra.testProgramDesc)}.txt`;

        let csvStr = await toCSV({
          data: item.testResults,
          del: '\t',
          hasCSVColumnTitle: true
        });
        csvStr = csvStr.replace(/"/g, '');

        try {
          console.log(`truncating ${outputFilename}`)
          await fs.truncate(outputFilename);

          // input file name-import table name.file extension
          await asyncPrependFile(outputFilename, csvStr);
        } catch (e) {
          // input file name-import table name.file extension
          await asyncPrependFile(outputFilename, csvStr);
        }
      });

      return obs.skip(1).subscribe(async function(item) {
        console.log('in not first');
        console.log(item);
        let outputFilename = `output/${toFileName(item.extra.testProgramDesc)}.txt`;
        let hasCSVColumnTitle;
        let csvStr = await toCSV({
          data: item.testResults,
          del: '\t',
          hasCSVColumnTitle: false
        });
        console.log(csvStr);

        csvStr = csvStr.replace(/"/g, '');

        await fs.appendFile(outputFilename, `${EOL}${csvStr}`);
      });
    });
}
