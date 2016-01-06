require('babel-polyfill');

import {
  getStudentIdFromSsid,
  getStudentNumberFromSsid,
  getTestDcid
}
from './service';
import {
  createWriteStream
}
from 'fs';
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
import json2csv from 'json2csv';
import util from 'util';
import {
  logger
}
from './index';

import {
  EOL
}
from 'os';

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

        Observable.fromPromise(Promise.all(asyncProps))
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
      let notEmpty = !isEmpty(item.testResults) && !isEmpty(item.extra);
      return notEmpty;
    });
}

/**
 * converts a test_program_desc value to a PS.Test.name value
 * @param  {string} testProgramDesc PS.Test.name
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
    .subscribe(groupedObservable => {

      let ws;
      toCsvObservable(groupedObservable).subscribe(item => {
        if (!ws && item.ws) {
          ws = item.ws;
        }
        ws.write(item.csv);
      });

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
function toCsvObservable(srcObservable) {
  return srcObservable.concatMap(async function(item, i) {
    if (i === 0) {
      let outputFilename = `output/${toFileName(item.extra.testProgramDesc)}.txt`;

      // creates file if it doesn't exist
      var ws = createWriteStream(outputFilename, {
        flags: 'a'
      });

      await fs.truncate(outputFilename);
    }

    return toCSV({
        data: item.testResults,
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
