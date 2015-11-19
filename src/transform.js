require('babel-polyfill');
import through from 'through';
import { getMatchingStudentTest } from './service';
import _ from 'lodash';
import { inspect } from 'util';
import Dibels from './testClasses';
import json2csv from 'json2csv';
import Bluebird from 'bluebird';
import fs from 'fs-promise';

var toCSV = Bluebird.promisify(json2csv);

export default async function transform(csvData, testId) {
  try {
    let studentTests = await getMatchingStudentTest(csvData['Student Primary ID'], csvData['School Year'], testId);
    if (!studentTests.rows.length) {
      let dibels = new Dibels(csvData, testId);
      let importCsv = await dibels.toImportCsv();
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
