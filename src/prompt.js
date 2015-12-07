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
import transform from './transform';
import json2csv from 'json2csv';
import {
  basename, extname
}
from 'path';
import {
  EOL
}
from 'os';
import workflow from './workflow';
import {
  Observable,
  Node
}
from '@reactivex/rxjs';


var toCSV = Bluebird.promisify(json2csv);


function asyncPrompt(questions) {
  return new Promise((resolve, reject) => {
    prompt(questions, function(answers) {
      resolve(answers);
    });
  });
}

export default async function promptHandler(source) {
  try {
    let matchingTests = await getMatchingTests('ROGL');
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

    let importQuestion = {
      type: 'list',
      name: 'table',
      message: 'Which table/set are you forging import data for?',
      choices: [{
        name: 'test-results',
        value: 'Test Results'
      }, {
        name: 'u-proficiency',
        value: 'U_StudentTestProficiency'
      }, {
        name: 'u-subscore',
        value: 'U_StudentTestSubscore'
      }]
    };

    let table = await asyncPrompt(importQuestion);
    console.log(`Creating test data for table: ${table.table}`);
    let promptResps = {
      testId: testId,
      table: table.table
    }
    workflow(source, promptResps);
  } catch (e) {
    console.error(e.stack);
  }
}
