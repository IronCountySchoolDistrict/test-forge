require('babel-polyfill');

import {
  getStudentId,
  getTestDcid,
  getMatchingStudentTestScore
}
from './service';

import fs from 'fs-promise';

class CRT {
  constructor(record, testId) {
    this.testId = testId;
  }

  get testDcid() {
    return getTestDcid(this.testId)
      .then(testDcid => new Promise((resolve, reject) => {
        resolve(testDcid);
      }));
  }
}
