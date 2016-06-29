require('babel-polyfill');

/* external imports */
import { createWriteStream, truncateSync } from 'fs';
import Promise from 'bluebird';
import { Observable } from '@reactivex/rxjs';
import { isEmpty } from 'lodash';
import { gustav } from 'gustav';
import { merge, uniq, uniqWith, isEqual, truncate, extend, find } from 'lodash';

/* couplers */
import { SamsCoupler } from './couplers/sams';
import { PowerSchoolCoupler } from './couplers/powerschool';
import { CsvCoupler } from './couplers/csv';

/* transformers */
import { testResultConceptTransform } from './transformers/crt/testResultConcept';
import { testScoreTransform } from './transformers/crt/testScore';
import { testResultTransform } from './transformers/crt/testResult';
import { proficiencyTransform } from './transformers/crt/proficiency';

import {
  ssidToStudentNumber,
  testRecordToMatchingDcid,
  testNameToDcid,
  studentTestScoreDuplicateCheck,
  ssidsToStudentIds
}
from './blogic';
import {
  getStudentIdsFromSsidBatch,
  getTestIdsFromNamesBatch,
  getStudentIdsFromSsidBatchDual
} from './service';
import { logger } from './index';
import { printObj } from './util';


function logErrors(item, msg, e) {
  logger.log('info', msg, {
    psDbError: printObj(e)
  });
  if (item) {
    logger.log('info', 'Source Data Record: ', {
      sourceData: printObj(item)
    });
  }
}

export function createWorkflow(prompt) {
  gustav.addCoupler(new SamsCoupler());
  gustav.addCoupler(new PowerSchoolCoupler());
  gustav.addCoupler(new CsvCoupler());

  let params = {};
  if (prompt.dest === 'Database') {
    params.coupler = 'powerschool';
    params.channel = prompt.table;
  } else if (prompt.dest === 'CSV') {
    params.coupler = 'csv';
    params.channel = {
      prompt: prompt
    };
    if (prompt.table === 'Test Result') {
      params.channel.groupByProperty = 'extra.testProgramDesc';
      params.channel.outputProperty = 'csvOutput';
    } else if (prompt.table === 'Test Result Concept') {
      params.channel.groupByProperty = '';
      params.channel.outputProperty = '';
    } else if (prompt.table === 'U_StudentTestProficiency') {
      params.channel.groupByProperty = 'extra.testProgramDesc';
      params.channel.outputProperty = 'csvOutput';
    } else if (prompt.table === 'Test Score') {
      params.channel.groupByProperty = 'test_program_desc';
      params.channel.outputProperty = ''; // use the entire object
    }
  }

  const workflow = gustav.createWorkflow()
    .from('sams', prompt.table)
    .transf(transformer, prompt)
    .to(...Object.keys(params).map(k => params[k]));

  return workflow;
}

function transformer(config, observer) {
  if (config.table === 'Test Result') {
    return testResultTransform(observer);
  }
  if (config.table === 'Test Result Concept') {
    return testResultConceptTransform(observer);
  }
  if (config.table === 'U_StudentTestProficiency') {
    return proficiencyTransform(observer);
  }
  if (config.table === 'Test Score') {
    return crtTestScoreTransform(observer);
  }
}

function consoleNode(observable) {
  return observable.subscribe(x => console.dir(x));
}
