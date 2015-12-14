require('babel-polyfill');

import {
  getStudentIdFromSsid,
  getStudentNumberFromSsid,
  getTestDcid,
  getMatchingStudentTestScore
}
from './service';

import Promise from 'bluebird';
import fs from 'fs-promise';
import { Observable } from '@reactivex/rxjs';
import { asyncPrependFile } from './workflow';

export class CRTTestResults {
  constructor(record, promptOpts) {
    this.testId = promptOpts.testId;
    this.ssid = record.ssid;
    this.schoolYear = record.school_year;
    this.gradeLevel = record.grade_level;
    this.compositeScore = record.test_overall_score;
    this.testProgramDesc = record.test_program_desc;
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
      console.error(e.trace);
    }
  }

  async toTestResultsCsv() {
    let asyncProps = await Promise.all([this.studentId, this.studentNumber]);

    return {
        'Test Date': this.schoolYear,
        'Student Id': asyncProps[0],
        'Student Number': asyncProps[1],
        'Grade Level': this.gradeLevel,
        'Composite Score Alpha': this.compositeScore
    };
  }

  createTransformer(promptOpts, record) {
    return Observable.fromPromise(this.toTestResultsCsv());
  }

  /**
   * converts a test_program_desc value to a PS.Test.name value
   * @param  {[type]} testProgramDesc [description]
   * @return {[type]}                 [description]
   */
  outputFilename(testProgramDesc) {
    if (testProgramDesc.indexOf('Grade Language Arts') !== -1) {
      return 'CRT - Language Arts';
    } else if (testProgramDesc.indexOf('Grade Science') !== -1) {
      return 'CRT - Science';
    } else if (testProgramDesc.indexOf('Grade Math') !== -1) {
      return 'CRT - Math';
    } else if (testProgramDesc.indexOf('Algebra I') !== -1) {
      return 'CRT - Algebra I';
    }
  }

  createCsvSink(config, observer) {

    let testProgSource = observer.groupBy(
      x => x.extra.testProgramDesc,
      x => x
    );

    testProgSource.subscribe(item => {
      console.log(`item == ${item}`);
    });

    // observer.first().subscribe(async function(item) {
    //   let csvStr = await toCSV({
    //     data: item,
    //     del: '\t',
    //     hasCSVColumnTitle: true
    //   });
    //   csvStr = csvStr.replace(/"/g, '');
    //
    // });
    //
    // return observer.subscribe(async function(item) {
    //   let outputFilename = `output/${this.outputFilename(item.extra.testProgramDesc)}`;
    //   let hasCSVColumnTitle;
    //   let fileStat = await fs.stat(outputFilename);
    //
    //   try {
    //     await fs.truncate(outputFilename);
    //     await fs.asyncPrependFile(`output/${outputFilename}`, `${csvStr}`);
    //   } catch (e) {
    //     // output CSV file does not exist, prepend first record into new file
    //     // input file name-import table name.file extension
    //
    //     await asyncPrependFile(`output/${outputFilename}`, `${csvStr}`);
    //   }
    //   let csvStr = await toCSV({
    //     data: item,
    //     del: '\t',
    //     hasCSVColumnTitle: true
    //   });
    //
    //   csvStr = csvStr.replace(/"/g, '');
    //
    //   await fs.appendFile(`output/${outputFilename}`, `${EOL}${csvStr}`);
    // });
  }
}
