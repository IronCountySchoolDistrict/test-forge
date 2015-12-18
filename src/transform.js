require('babel-polyfill');
import through from 'through';
import { getMatchingStudentTest } from './service';
import _ from 'lodash';
import { inspect } from 'util';
import Dibels from './testClasses';
import json2csv from 'json2csv';
import Bluebird from 'bluebird';
import fs from 'fs-promise';
import detect from './detector';

var toCSV = Bluebird.promisify(json2csv);

export default async function transform(csvData, testId, importTable) {
  try {
    let importCsv;
    let testObj;
    let testType = detect(csvData);
    if (testType === 'dibels') {
      testObj = new Dibels(csvData, testId);
      if (importTable === 'Test Results') {
        let studentTests = await getMatchingStudentTest(csvData['Student Primary ID'], csvData['School Year'], testId);
        if (!studentTests.rows.length) {
          importCsv = await testObj.toTestResultsCsv();
        }
      } else if (importTable === 'U_StudentTestProficiency') {
        importCsv = await testObj.toProficiencyCsv();
      }
    }

    if (importCsv) {
      let csvStr = await toCSV({
        data: importCsv,
        del: '\t',
        hasCSVColumnTitle: false
      });
      csvStr = csvStr.replace(/"/g, '');
      return {
        csvStr: csvStr,
        importCsv: importCsv
      };
    } else {
      return {};
    }

  } catch (e) {
    console.error(e.stack);
  }
}
