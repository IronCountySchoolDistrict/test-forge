require('babel-polyfill');

import {
  prompt
}
from 'inquirer';
import {
  getMatchingTests
}
from './service';
import through from 'through';
import {
  createReadStream
}
from 'fs';
import {
  Writable
}
from 'stream';
import readFile from 'fs-readfile-promise';
import csv from 'csv';
import transform from './transform';

function asyncPrompt(questions) {
  return new Promise(function(resolve, reject) {
    prompt(questions, function(answers) {
      resolve(answers);
    });
  });
}

export default async function promptHandler(file) {
  var testId;
  try {
    let matchingTests = await getMatchingTests('ROGL');
    let testChoices = matchingTests.rows.map(test => ({
      name: test.NAME,
      value: test.ID
    }));
    console.log(testChoices);

    var testQuestion = {
      type: 'list',
      name: 'tests',
      message: 'Which test would you like to use for the import?',
      choices: testChoices
    };

    let answer = await asyncPrompt(testQuestion);
    console.log(`Using test with id: ${answer.tests}`);
    var tableQuestion = {
      type: 'list',
      name: 'tables',
      message: 'Which table do you want to generate a csv file for?',
      choices: ['StudentTest', 'U_StudentTestProficiency', 'U_StudentTestSubscore']
    };

    answer = await asyncPrompt(tableQuestion);
    console.log(`Generating csv for table ${answer.tables}`);

  } catch (e) {
    console.log(e);
  }
}
