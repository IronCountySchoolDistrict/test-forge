require('babel-polyfill');

import { prompt } from 'inquirer';
import json2csv from 'json2csv';
import { Observable } from '@reactivex/rxjs';
import Bluebird from 'bluebird';

import detect from './detector';
import * as sage from './sage';
import * as crt from './crt';
import * as dibels from './dibels';
import { getMatchingTests, getTestFromName, getCrtTestResults, getCrtProficiency, getCrtTestScores } from './service';

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
          name: 'Test Scores',
          value: 'Test Scores'
        }]
    };

    let { table } = await prompt([importQuestion]);

    let destQuestion = {
      type: 'list',
      name: 'dest',
      message: 'Please choose the destination for your data',
      choices: [{
        name: 'CSV',
        value: 'CSV'
      }, {
        name: 'Database',
        value: 'Database'
      }]
    };

    let { dest } = await prompt([destQuestion]);

    let promptResps = {
      table,
      dest
    };

    const workflow = crt.createWorkflow(promptResps);

    workflow.start();
  } catch (e) {
    console.error(e.stack);
  }
}
