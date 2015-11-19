require('babel-polyfill');

import { prompt } from 'inquirer';
import {
  getMatchingTests,
  getStudentTests
}
from './service';
import through from 'through';
import { createReadStream } from 'fs';
import { Writable } from 'stream';
import fs from 'fs-promise';
import csv from 'csv';
import transform from './transform';
import byline from 'byline';
import _ from 'lodash';
import Bluebird from 'bluebird';
import json2csv from 'json2csv';
import { basename } from 'path';
import { EOL } from 'os';

var toCSV = Bluebird.promisify(json2csv);

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

    let readStream = createReadStream(file);
    let parser;
    let printColumns = true;
    let outputFilename = basename(file);
    try {
      let fileStat = await fs.stat(`output/${outputFilename}`);
      await fs.truncate(`output/${outputFilename}`, 0);
    } catch (e) {
      console.log('output file not found, don\'t need to truncate output file');
    }

    readStream.on('data', chunk => {
      let columns;
      parser = csv.parse({
        delimiter: '\t',
        skip_empty_lines: true
      });
      parser.on('readable', async function() {
        // If this is the first time the parser is running, assume this is the
        // column row
        if (!columns) {
          columns = parser.read();
        } else {
          let csvRowObj = _.zipObject(columns, parser.read());
          // Check if all values of csvRowObj is completely empty (undefined)
          let isEmpty = _.reduce(csvRowObj, (result, n, key) => {
            return result && !csvRowObj[key];
          }, true);

          if (!isEmpty) {
            let transformed = await transform(csvRowObj, testId, printColumns);
            if (printColumns) {
              fs.appendFile(`output/${outputFilename}`, Object.keys(transformed.importCsv).join('\t'));
              printColumns = false;
            }
            fs.appendFile(`output/${outputFilename}`, `${EOL}${transformed.csvStr}`);
          }
        }
      });
      parser.write(chunk.toString());
    });

    readStream.on('end', () => {
      parser.end();
    })

  } catch (e) {
    console.error(e.stack);
  }
}
