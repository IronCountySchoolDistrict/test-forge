require('babel-polyfill');
import through from 'through';
import {
  getStudentTests,
  getSchoolId
}
from './service';
import _ from 'lodash';
import {inspect} from 'util';
import Dibels from './testClasses';

export default async function transform(csvData, testId) {
  try {
    let studentTests = await getStudentTests(csvData['Student Primary ID'], csvData['School Year'], testId);
    if (studentTests.rows.length === 1) {
      let studentTest = studentTests.rows[0];
      let { rows } = await getSchoolId(csvData['School Name']);
      let dibels = new Dibels(csvData, testId, studentTest);
      let importCsv = await dibels.toImportCsv(studentTest);
      console.log(importCsv);
    } else if (!studentTest.rows.length) {
      console.log('found zero studentTest records, creating new one');
    } else if (studentTest.rows.length > 1) {
      console.error(`More than one studentTests record found for Student: ${csvData['Student Primary ID']}`);
    }
  } catch (e) {
    console.log(e.trace);
  }
}
