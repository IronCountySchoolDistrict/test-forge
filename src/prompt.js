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
import byline from 'byline';

function asyncPrompt(questions) {
  return new Promise(function(resolve, reject) {
    prompt(questions, function(answers) {
      resolve(answers);
    });
  });
}

export default async function promptHandler(file) {
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
    let tableQuestion = {
      type: 'list',
      name: 'tables',
      message: 'Which table do you want to generate a csv file for?',
      choices: ['StudentTest', 'U_StudentTestProficiency', 'U_StudentTestSubscore']
    };

    let tables = await asyncPrompt(tableQuestion);
    console.log(`Generating csv for table ${tables.tables}`);

    let parser = csv.parse({
      delimiter: '\t',
      columns: true
    });
    let readStream = createReadStream(file);
    let fileArray = [];
    readStream.on('data', chunk => {
      parser.on('readable', () => {
        let csvData = parser.read();
        transform(csvData, testId);
      });
      parser.write(chunk.toString());
    });

  } catch (e) {
    console.error(e.trace);
  }
}
