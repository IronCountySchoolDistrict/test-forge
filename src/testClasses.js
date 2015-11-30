require('babel-polyfill');

import {
  getStudentId,
  getMatchingStudentTestScore
}
from './service';

import fs from 'fs-promise';

// Object representation of the Dibels csv data for each student
export default class Dibels {
  constructor(record, testId) {
    console.log(record);
    this.gradeLevel = record['Grade'];
    this.studentPrimaryId = record['Student Primary ID'];
    this.compositeScore = record['Composite Score'];
    this.benchmark = record['Assessment Measure-Composite Score-Levels'];
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
   * @return {object} Object that will be serialized for the csv file
   */
  async toTestResultsCsv() {
    try {
      let config = await fs.readFile('./config.json');
      let configObj = JSON.parse(config.toString());

      let csvObj = {
        'Test Date': configObj.testConstants.ROGL_Begin_Year.testDate,
        'Student Id': await this.studentId,
        'Student Number': this.studentPrimaryId,
        'Grade Level': this.gradeLevel,
        'Composite Score Alpha': this.compositeScore
      }
      return csvObj;
    } catch (e) {
      console.error(e.stack);
    }
  }

  async toProficiencyCsv() {
    try {
      let studentTestScore = await getMatchingStudentTestScore();
      let studentTestScoreDcid = studentTestScore.dcid;

      return {
        'studentTestScoreDcid': studentTestScoreDcid,
        'proficiency': this.proficiency
      }
    }
    catch (e) {
      console.error(e.stack);
    }
  }
}
