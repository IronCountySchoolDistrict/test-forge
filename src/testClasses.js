require('babel-polyfill');

import {
  getSchoolId,
  getStudentId
}
from './service';

// Object representation of the Dibels csv data for each student
export default class Dibels {
  constructor(record, testId) {
    this.gradeLevel = record['Grade'];
    this.studentPrimaryId = record['Student Primary ID'];
    this.testId = testId;
    this.compositeScore = record['Composite Score'];
  }

  get schoolId() {
    return getSchoolId(this.schoolName)
      .then(r => {
        try {
          if (r.rows.length > 1) {
            throw new Error('Expected getSchoolId() to return one row, got back more than one record');
          }
          return new Promise((resolve, reject) => {
            resolve(r.rows[0].SCHOOL_NUMBER);
          });
        } catch (e) {
          console.error(e.trace);
        }
      });
  }

  get studentId() {
    try {
      return getStudentId(this.studentPrimaryId)
        .then(r => {
          if (r.rows.length > 1) {
            throw new Error('Expected getStudentId() to return one row, got back more than one record');
          }
          return new Promise((resolve, reject) => {
            resolve(r.rows[0].ID);
          });
        });
    } catch (e) {
      console.error(e.trace);
    }
  }

  /**
   * Returns an object that represents the data
   * that will be written to a csv file for import.
   * @param  {object} [studentTest] Check the existing studentTest record for values to update
   * @return {object} Object that will be serialized for the csv file
   */
  async toImportCsv() {
    try {
      console.log('no studentTest param passed in, using csv values by default');
      return {
        studentId: await this.studentId,
        studentNumber: this.studentPrimaryId,
        gradeLevel: this.gradeLevel,
        compositeScoreAlpha: this.compositeScore
      }
    } catch (e) {
      console.log(e.trace);
    }
  }
}
