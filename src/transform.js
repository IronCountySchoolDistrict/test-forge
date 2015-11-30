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

export default async function transform(csvData, testId) {
  try {
    console.log(csvData);
    let studentTests = await getMatchingStudentTest(csvData['Student Primary ID'], csvData['School Year'], testId);
    if (!studentTests.rows.length) {
      let testType = detect(csvData);

      let testObj;
      if (testType === 'dibels') {
        testObj = new Dibels(csvData, testId);
      }
      let importCsv = await testObj.toTestResultsCsv();
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
    }
  } catch (e) {
    console.error(e.stack);
  }
}
