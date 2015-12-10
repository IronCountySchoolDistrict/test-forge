require('babel-polyfill');

import {
  prompt
}
from 'inquirer';
import {
  getMatchingTests,
  getStudentTests
}
from './service';
import detect from './detector';
import transform from './transform';
import json2csv from 'json2csv';
import workflow from './workflow';
import {
  Observable,
  Node
}
from '@reactivex/rxjs';
import Bluebird from 'bluebird';

var toCSV = Bluebird.promisify(json2csv);

function asyncPrompt(questions) {
  return new Promise((resolve, reject) => {
    prompt(questions, function(answers) {
      resolve(answers);
    });
  });
}

export default async function promptHandler(source, file) {
  try {
    source.take(1).subscribe(async function(csvData) {
      let test = detect(csvData);
      console.log(test);
      let matchingTests = await getMatchingTests(test.name);
      let testChoices = matchingTests.rows.map(test => ({
        name: test.NAME,
        value: test.ID
      }));

      let testQuestion = {
        type: 'list',
        name: 'tests',
        message: 'Which test would you like to use for the import?',
        choices: testChoices
      };

      let tests = await asyncPrompt(testQuestion);
      console.log(`Using test with id: ${tests.tests}`);
      let testId = tests.tests;

      // TODO: Make this list dynamic, based on the type of data source given.
      let importQuestion = {
        type: 'list',
        name: 'table',
        message: 'Which table/set are you forging import data for?',
        choices: [{
          name: 'test-results',
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

      let table = await asyncPrompt(importQuestion);
      console.log(`Creating test data for table: ${table.table}`);
      let promptResps = {
        testId: testId,
        table: table.table
      }

      workflow(source, promptResps, file);
    });
  } catch (e) {
    console.error(e.stack);
  }
}
