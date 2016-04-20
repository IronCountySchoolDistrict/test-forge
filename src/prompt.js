require('babel-polyfill');

import { prompt } from 'inquirer';
import json2csv from 'json2csv';
import { Observable } from '@reactivex/rxjs';
import Bluebird from 'bluebird';

import detect from './detector';
import * as sage from './sage';
import * as crt from './crt';
import * as dibels from './dibels';
import { getMatchingTests, getTestFromName, getCrtTestResults, getCrtProficiency } from './service';

var toCSV = Bluebird.promisify(json2csv);

async function promptTestId(test) {
  let matchingTests = await getMatchingTests(test.name);
  let testChoices = matchingTests.rows.map(test => ({
    name: test.NAME,
    value: test.ID
  }));
  let testQuestion = {
    type: 'list',
    name: 'testId',
    message: 'Which test would you like to use for the import?',
    choices: testChoices
  };

  let importTest = await prompt([testQuestion]);

  console.log(`Using test with id: ${importTest.testId}`);
  return importTest.testId;
}

async function promptTable() {
  // TODO: Make this list dynamic, based on the test type of the data source.
  let importQuestion = {
    type: 'list',
    name: 'table',
    message: 'Which table/data set are you forging import data for?',
    choices: [
      {
        name: 'Test Results',
        value: 'Test Results'
      },
      {
        name: 'U_StudentTestProficiency',
        value: 'U_StudentTestProficiency'
      }
    ]
  };

  let importTable = await prompt([importQuestion]);
  console.log(`Creating test data for table/data set: ${importTable.table}`);
  return importTable.table;
}

export async function promptHandlerFile(source, file) {
  try {
    source.take(1).subscribe(async function(csvData) {
      let test = detect(csvData);

      if (test.name === 'ROGL') {
        let testId = await promptTestId(test);
        let table = await promptTable();
        let promptResps = {
          testId: testId,
          table: table
        };
        var workflow = dibels.createWorkflow(source, promptResps, file);
      } else if (test.name === 'SAGE') {
        let table = await promptTable();
        let promptResps = {
          table: table
        }
        var workflow = sage.createWorkflow(source, promptResps);
      }
      workflow.start();
    });
  } catch (e) {
    console.error(e.stack);
  }
}

export async function promptHandlerSams(test) {
  try {
    // TODO: Make this list dynamic, based on the type of data source given.
    let importQuestion = {
      type: 'list',
      name: 'table',
      message: 'Which table/set are you forging import data for?',
      choices: [{
        name: 'Test Results',
        value: 'Test Results'
      }, {
          name: 'U_StudentTestProficiency',
          value: 'U_StudentTestProficiency'
        }, {
          name: 'U_StudentTestSubscore',
          value: 'U_StudentTestSubscore'
        }, {
          name: 'U_TestSubscore',
          value: 'U_TestSubscore'
        }]
    };

    let table = await prompt([importQuestion]);
    console.log(`Creating test data for table: ${table.table}`);
    let promptResps = {
      table: table.table
    };

    let source;
    if (promptResps.table === 'Test Results') {
      source = await getCrtTestResults();
    } else {
      source = await getCrtProficiency();
    }
    source.count().subscribe(x => console.log('count == ', x));

    let workflow = crt.createWorkflow(source, promptResps);
    workflow.start();
  } catch (e) {
    console.error(e.stack);
  }
}
