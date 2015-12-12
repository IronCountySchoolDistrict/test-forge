require('babel-polyfill');

import {
  getStudentIdFromSsid,
  getStudentNumberFromSsid,
  getTestDcid,
  getMatchingStudentTestScore
}
from './service';

import fs from 'fs-promise';
import { Observable } from '@reactivex/rxjs';

export class CRTTestResults {
  constructor(record, promptOpts) {
    this.testId = promptOpts.testId;
    this.ssid = record.ssid;
    this.schoolYear = record.school_year;
    this.gradeLevel = record.grade_level;
    this.compositeScore = record.test_overall_score;
  }

  get studentId() {
    try {
      return getStudentIdFromSsid(this.ssid)
        .then(r => {
          return new Promise((resolve, reject) => {
            console.log('settled Promise for studentId()');
            if (r.rows.length > 1) {
              reject(new Error('Expected getStudentId() to return one row, got back more than one record'));
            }
            resolve(r.rows[0].ID);
          });
        });

    } catch (e) {
      console.log('in studentId catch');
      console.error(e.trace);
    }
  }

  get studentNumber() {
    try {
      return getStudentNumberFromSsid(this.ssid)
        .then(r => {
          return new Promise((resolve, reject) => {
            console.log('settled Promise for studentNumber()');
            if (r.rows.length > 1) {
              reject(new Error('Expected getStudentDcidFromSsid() to return one row, got back more than one record'));
            }
            resolve(r.rows[0].STUDENT_NUMBER);
          });
        })

    } catch (e) {
      console.log('in studentNumber catch');
      console.error(e.trace);
    }
  }

  async toTestResultsCsv() {
    console.log('returning test results');
    //TODO: studentNumber is returning later than studentId
    //convert the two Promises above to Observables and let both
    //db queries start at the same time.
    return {
      'Test Date': this.schoolYear,
      'Student Id': await this.studentId,
      'Student Number': await this.studentNumber,
      'Grade Level': this.gradeLevel,
      'Composite Score Alpha': this.compositeScore
    }
  }

  createTransformer(promptOpts, record) {
    return Observable.fromPromise(this.toTestResultsCsv());
  }
}
