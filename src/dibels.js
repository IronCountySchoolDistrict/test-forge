require('babel-polyfill');

import {
  getStudentId,
  getMatchingStudentTest,
  getMatchingStudentTestScore
}
from './service';

import fs from 'fs-promise';
import { Observable } from '@reactivex/rxjs';
import Promise from 'bluebird';

export default class Dibels {
  constructor(record, promptOpts) {
    this.testId = promptOpts.testId;
    this.gradeLevel = record['Grade'];
    this.studentPrimaryId = record['Student Primary ID'];
    this.compositeScore = record['Composite Score'];
    this.benchmark = record['Assessment Measure-Composite Score-Levels'];
    this.termName = record['School Year'];
  }

  get studentId() {
    try {
      return getStudentId(this.studentPrimaryId)
        .then(r => {
          return new Promise((resolve, reject) => {
            if (r.rows.length > 1) {
              reject(new Error('Expected getStudentId() to return one row, got back more than one record'));
            }
            resolve(r.rows[0].ID);
          });
        });
    } catch (e) {
      console.error(e.trace);
    }
  }

  get studentTestScoreDcid() {
    try {
      return getMatchingStudentTestScore(
          this.studentPrimaryId,
          this.termName,
          this.compositeScore,
          this.testId
        )
        .then(r => {
          return new Promise((resolve, reject) => {
            if (r.rows.length !== 1) {
              reject(new Error(`Expected studentTestScoreDcid() to return only one row, got back ${r.rows.length} rows`));
            }
            resolve(r.rows[0].DCID)
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

  /** Converts the dibels object into a Proficiency data object
   * @return {object}
   */
  async toProficiencyCsv() {
    try {
      return {
        'studentTestScoreDcid': await this.studentTestScoreDcid,
        'benchmark': this.benchmark
      }
    } catch (e) {
      return e;
    }
  }

  /**
   * create an Observable that outputs the data determined by the options
   * passed in by the user
   * @return {Obserable} Observable that emits data matching the format
   * asked by the user
   */
  createTransformer(promptOpts, record) {
    if (promptOpts.table === 'Test Results') {
      return Observable.fromPromise(
        getMatchingStudentTest(record['Student Primary ID'], record['School Year'], promptOpts.testId)
      ).flatMap(studentTests => {
        if (!studentTests.rows.length) {
          return Observable.fromPromise(this.toTestResultsCsv());
        } else {
          return new Promise.resolve({});
        }
      });
    } else if (promptOpts.table === 'U_StudentTestProficiency') {
      return Observable.fromPromise(this.toProficiencyCsv());
    } else if (promptOpts.table === 'U_StudentTestSubscore') {
      return Observable.fromPromise(new Promise((resolve, reject) => {
        resolve({});
      }));
    } else if (promptOpts.table === 'U_TestSubscore') {
      return Observable.fromPromise(new Promise((resolve, reject) => {
        resolve({});
      }));
    }
  }
}
