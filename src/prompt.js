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
import { basename, extname } from 'path';
import { EOL } from 'os';
import { exec } from 'child_process';
import ProgressBar from 'progress';

var toCSV = Bluebird.promisify(json2csv);
var asyncExec = Bluebird.promisify(exec);

function asyncPrompt(questions) {
  return new Promise((resolve, reject) => {
    prompt(questions, function(answers) {
      resolve(answers);
    });
  });
}

function asyncCsvParse(str, options) {
  return new Promise((resolve, reject) => {
    csv.parse(str, options, (err, output) => {
      if (err) {
        reject(err);
      } else if (output) {
        resolve(output[0]);
      }
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

    let importQuestion = {
      type: 'list',
      name: 'imports',
      message: 'Which table(s) are you forging import data for?',
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

    let imports = await asyncPrompt(importQuestion);
    console.log(`Forging import data for ${imports.imports}`);
    let importTable = imports.imports;

    let readStream = byline(createReadStream(file));
    let printColumns = true;

    let cmd = `wc -l ${file} | cut -f1 -d' '`;
    let numLines = await asyncExec(cmd);
    numLines = parseInt(numLines);
    let bar = new ProgressBar('  forging test data [:bar] :percent :etas', {
      complete: '=',
      incomplete: ' ',
      width: 40,
      total: numLines
    });
    let columns;

    readStream.on('data', async function(chunk) {
      let csvOpts = {
        delimiter: '\t'
      };
      if (!columns) {
        // Pause the stream here so asyncCsvParse finishes before the next data event
        // is fired. Without pausing here, the readStream would move on to the following rows
        // before the column row has finished parsing.
        readStream.pause();
        columns = await asyncCsvParse(chunk.toString(), csvOpts);
        readStream.resume();
      } else {
        csvOpts.columns = columns;
        let csvObj = await asyncCsvParse(chunk.toString(), csvOpts);
        let transformed = await transform(csvObj, testId, importTable);

        let outputFilename = basename(file, extname(file)) + '-' + importTable + extname(file);

        try {
          await fs.truncate(`output/${outputFilename}`, 0);
        } catch (e) {
          console.error(e.stack);
        }
        if (transformed) {
          if (printColumns) {
            // Pause the stream so columns finish printing to the csv file
            // before the following row(s) are appended to the file
            printColumns = false;
            readStream.pause();

            // Print columns and first row of output
            await fs.appendFile(`output/${outputFilename}`, Object.keys(transformed.importCsv).join('\t'));
            await fs.appendFile(`output/${outputFilename}`, `${EOL}${transformed.csvStr}`);
            readStream.resume();
          } else {
            await fs.appendFile(`output/${outputFilename}`, `${EOL}${transformed.csvStr}`);
          }
        }
      }
    });
  } catch (e) {
    console.error(e.stack);
  }
}
