// Collection of database queries
import Promise from 'bluebird';
import orawrap from 'orawrap';
import {Observable} from '@reactivex/rxjs';
import cache from 'memory-cache';

import {execute, msExecute} from './database';
import {logger} from './index';

export function getMatchingStudentTest(studentNumber, termName, testId) {
  return execute(`
    SELECT studenttest.*
    FROM studenttest
      JOIN students ON studenttest.studentid = students.id
      JOIN terms ON studenttest.termid = terms.id AND
                    studenttest.schoolid = terms.schoolid
    WHERE students.student_number = :student_number
          AND (SELECT yearid
                FROM terms
                WHERE studenttest.termid = terms.id AND studenttest.schoolid = terms.schoolid) =
              (SELECT yearid
                FROM terms
                WHERE
                  name = :term_name AND
                  schoolid = studenttest.schoolid)
          AND studenttest.testid=:testId
              `, [studentNumber, termName, testId], {
    outFormat: orawrap.OBJECT
  });
}

/**
 * get the PS.StudentTestScore record that matches the data passed in
 * @return {Promise}              returns a Promise that resolves with the results
 * if exactly one match is found. If zero matches, or more than match,
 * is found, reject with an error message.
 */
export function getMatchingStudentTestScore(studentNumber, termName, score, testId) {
  return execute(`
    SELECT studenttestscore.dcid
    FROM studenttestscore
      JOIN studenttest ON studenttest.id = STUDENTTESTSCORE.STUDENTTESTID
      JOIN students on studenttest.STUDENTID=students.id
    WHERE students.student_number = :student_number
          AND (studenttestscore.alphascore = :score OR
               studenttestscore.numscore = :score OR
               studenttestscore.percentscore = :score)
          AND studenttest.termid IN
              (
                SELECT DISTINCT id
                FROM terms
                WHERE yearid = (SELECT DISTINCT yearid
                                FROM terms
                                WHERE name = :term_name)
              )
          AND studenttest.testid = :test_id
    `, {
    student_number: studentNumber,
    score: score,
    term_name: termName,
    test_id: testId
  }, {
    outFormat: orawrap.OBJECT
  });
}

export function getMatchingProficiency(studentNumber, termName, alphaScore, testId) {
  return execute(`
    SELECT u_studenttestproficiency.id
    FROM studenttestscore
      JOIN studenttest ON studenttest.id = STUDENTTESTSCORE.STUDENTTESTID
      JOIN students ON studenttest.STUDENTID = students.id
      JOIN u_studenttestproficiency ON STUDENTTESTSCORE.DCID = U_STUDENTTESTPROFICIENCY.STUDENTTESTSCOREDCID
    WHERE students.student_number = :student_number
          AND studenttestscore.alphascore = :alpha_score
          AND studenttest.termid IN
              (
                SELECT DISTINCT id
                FROM terms
                WHERE yearid = (SELECT DISTINCT yearid
                                FROM terms
                                WHERE name = :term_name)
              )
          AND studenttest.testid = :test_id
          `, [studentNumber, alphaScore, termName, testId], {
    outFormat: orawrap.OBJECT
  });
}

export function getTestFromName(searchTerm) {
  const rnd = Math.random() * 100;
  console.time(rnd);
  return execute(`
    SELECT id, name
    FROM test
    WHERE name = :searchTerm
    `, [searchTerm], {
    outFormat: orawrap.OBJECT,
    maxRows: 1
  })
    .then(result => {
      console.timeEnd(rnd);
      return result;
    });
}

export function getMatchingTests(searchTerm) {
  return execute(`
    SELECT id, name
    FROM test
    WHERE name LIKE '%' || :searchTerm || '%'
    `, [searchTerm], {
    outFormat: orawrap.OBJECT
  });
}

export function getTestDcid(testId) {
  return execute(`
    SELECT dcid FROM test WHERE id=:testId
    `, [testId], {
    outFormat: orawrap.OBJECT
  });
}

export function getStudentIdsFromSsidBatch(ssids) {
  return execute(`
    SELECT
      state_studentnumber,
      student_number
    FROM students
    WHERE state_studentnumber IN (:ssids)`,
    [ssids.join(',')],
    {outFormat: orawrap.OBJECT}
  );
}

export function getStudentIdsFromSsidBatchDual(ssids) {
  try {
    return execute(`
    SELECT
      ssid_input.ssid AS ssid,
      students.id AS student_id,
      students.student_number AS student_number
    FROM (
           SELECT REGEXP_SUBSTR(
                      :ssids,
                      '[^,]+', 1, level) AS ssid
           FROM dual
           CONNECT BY REGEXP_SUBSTR(
                          :ssids,
                          '[^,]+', 1, level) IS NOT NULL
    ) ssid_input
    LEFT JOIN students ON students.state_studentnumber = ssid_input.ssid`,
      {
        ssids: {val: ssids.join(','), dir: orawrap.BIND_IN, type: orawrap.STRING}
      },
      {
        outFormat: orawrap.OBJECT,
        maxRows: ssids.length
      }
    );
  } catch (e) {
    console.error(e);
  }

}

export function getStudentIdFromStudentNumber(studentNumber) {
  return execute(`
    SELECT id
    FROM students
    WHERE student_number=:studentNumber
  `, [studentNumber], {
    outFormat: orawrap.OBJECT
  });
}

export function getStudentIdFromSsid(ssid) {
  return execute(`
    SELECT ID
    FROM students
    WHERE State_StudentNumber=:ssid
    AND State_StudentNumber is not null
  `, [ssid], {
    outFormat: orawrap.OBJECT
  });
}

export function getStudentNumberFromSsid(ssid) {
  return execute(`
    SELECT STUDENT_NUMBER
    FROM students
    WHERE State_StudentNumber=:ssid
    AND State_StudentNumber is not null
    AND Student_number is not null
  `, [ssid], {
    outFormat: orawrap.OBJECT
  });
}

export function getCrtTestResults() {
  return msExecute(`
    SELECT 
       [student_test].school_year,
       [student_master].ssid,
       [student_enrollment].grade_level,
       [student_test].test_overall_score,
       [test_program].test_program_desc
    FROM   [sams_2008].[dbo].[student_test]
          INNER JOIN [sams_2008].[dbo].[student_enrollment]
                  ON [sams_2008].[dbo].[student_test].[student_id] =
                                [sams_2008].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2008].[dbo].[student_test].school_year =
                          [sams_2008].[dbo]. [student_enrollment].school_year
                      AND [sams_2008].[dbo].[student_test].school_number =
                          [sams_2008].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2008].[dbo].[student_master]
                  ON [sams_2008].[dbo].[student_test].[student_id] =
                      [sams_2008].[dbo] .[student_master].[student_id]
          INNER JOIN [sams_2008].[dbo].[test_program]
                  ON [sams_2008].[dbo].[student_test].[test_prog_id] =
                                [sams_2008] .[dbo].[test_program].[test_prog_id]
                      AND [sams_2008].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2008].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [sams_2009].[dbo].[student_test]
          INNER JOIN [sams_2009].[dbo]. [student_enrollment]
                  ON [sams_2009].[dbo].[student_test].[student_id] =
                                [sams_2009].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2009].[dbo].[student_test].school_year =
                          [sams_2009].[dbo]. [student_enrollment].school_year
                      AND [sams_2009].[dbo].[student_test].school_number =
                          [sams_2009].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2009].[dbo].[student_master]
                  ON [sams_2009].[dbo].[student_test].[student_id] =
                      [sams_2009].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_2009].[dbo].[test_program]
                  ON [sams_2009].[dbo].[student_test].[test_prog_id] =
                                [sams_2009] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_2009].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2009].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [sams_2010].[dbo].[student_test]
          INNER JOIN [sams_2010].[dbo].[student_enrollment]
                  ON [sams_2010].[dbo].[student_test].[student_id] =
                                [sams_2010].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2010].[dbo].[student_test].school_year =
                          [sams_2010].[dbo]. [student_enrollment].school_year
                      AND [sams_2010].[dbo].[student_test].school_number =
                          [sams_2010].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2010].[dbo].[student_master]
                  ON [sams_2010].[dbo].[student_test].[student_id] =
                      [sams_2010].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_2010].[dbo].[test_program]
                  ON [sams_2010].[dbo].[student_test].[test_prog_id] =
                                [sams_2010] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_2010].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2010].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [sams_2011].[dbo].[student_test]
          INNER JOIN [sams_2011].[dbo]. [student_enrollment]
                  ON [sams_2011].[dbo].[student_test].[student_id] =
                                [sams_2011].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2011].[dbo].[student_test].school_year =
                          [sams_2011].[dbo]. [student_enrollment].school_year
                      AND [sams_2011].[dbo].[student_test].school_number =
                          [sams_2011].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2011].[dbo].[student_master]
                  ON [sams_2011].[dbo].[student_test].[student_id] =
                      [sams_2011].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_2011].[dbo].[test_program]
                  ON [sams_2011].[dbo].[student_test].[test_prog_id] =
                                [sams_2011] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_2011].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2011].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [sams_2012].[dbo].[student_test]
          INNER JOIN [sams_2012].[dbo].[student_enrollment]
                  ON [sams_2012].[dbo].[student_test].[student_id] =
                                [sams_2012].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2012].[dbo].[student_test].school_year =
                          [sams_2012].[dbo]. [student_enrollment].school_year
                      AND [sams_2012].[dbo].[student_test].school_number =
                          [sams_2012].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2012].[dbo].[student_master]
                  ON [sams_2012].[dbo].[student_test].[student_id] =
                      [sams_2012].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_2012].[dbo].[test_program]
                  ON [sams_2012].[dbo].[student_test].[test_prog_id] =
                                [sams_2012] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_2012].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2012].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [sams_2013].[dbo].[student_test]
          INNER JOIN [sams_2013].[dbo]. [student_enrollment]
                  ON [sams_2013].[dbo].[student_test].[student_id] =
                                [sams_2013].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2013].[dbo].[student_test].school_year =
                          [sams_2013].[dbo]. [student_enrollment].school_year
                      AND [sams_2013].[dbo].[student_test].school_number =
                          [sams_2013].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2013].[dbo].[student_master]
                  ON [sams_2013].[dbo].[student_test].[student_id] =
                      [sams_2013].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_2013].[dbo].[test_program]
                  ON [sams_2013].[dbo].[student_test].[test_prog_id] =
                                [sams_2013] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_2013].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2013].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [sams_merge].[dbo].[student_test]
          INNER JOIN [sams_merge].[dbo]. [student_enrollment]
                  ON [sams_merge].[dbo].[student_test].[student_id] =
                                [sams_merge].[dbo]. [student_enrollment].[student_id]
                      AND [sams_merge].[dbo].[student_test].school_year =
                          [sams_merge].[dbo]. [student_enrollment].school_year
                      AND [sams_merge].[dbo].[student_test].school_number =
                          [sams_merge].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_merge].[dbo].[student_master]
                  ON [sams_merge].[dbo].[student_test].[student_id] =
                      [sams_merge].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_merge].[dbo].[test_program]
                  ON [sams_merge].[dbo].[student_test].[test_prog_id] =
                                [sams_merge] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_merge].[dbo].[student_test].test_overall_score != 0
                      AND [sams_merge].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [Success_2008].[dbo].[student_test]
          INNER JOIN [Success_2008].[dbo]. [student_enrollment]
                  ON [Success_2008].[dbo].[student_test].[student_id] =
                                [Success_2008].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2008].[dbo].[student_test].school_year =
                          [Success_2008].[dbo]. [student_enrollment].school_year
                      AND [Success_2008].[dbo].[student_test].school_number =
                          [Success_2008].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2008].[dbo].[student_master]
                  ON [Success_2008].[dbo].[student_test].[student_id] =
                      [Success_2008].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2008].[dbo].[test_program]
                  ON [Success_2008].[dbo].[student_test].[test_prog_id] =
                                [Success_2008] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2008].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2008].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [Success_2009].[dbo].[student_test]
          INNER JOIN [Success_2009].[dbo]. [student_enrollment]
                  ON [Success_2009].[dbo].[student_test].[student_id] =
                                [Success_2009].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2009].[dbo].[student_test].school_year =
                          [Success_2009].[dbo]. [student_enrollment].school_year
                      AND [Success_2009].[dbo].[student_test].school_number =
                          [Success_2009].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2009].[dbo].[student_master]
                  ON [Success_2009].[dbo].[student_test].[student_id] =
                      [Success_2009].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2009].[dbo].[test_program]
                  ON [Success_2009].[dbo].[student_test].[test_prog_id] =
                                [Success_2009] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2009].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2009].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [Success_2010].[dbo].[student_test]
          INNER JOIN [Success_2010].[dbo]. [student_enrollment]
                  ON [Success_2010].[dbo].[student_test].[student_id] =
                                [Success_2010].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2010].[dbo].[student_test].school_year =
                          [Success_2010].[dbo]. [student_enrollment].school_year
                      AND [Success_2010].[dbo].[student_test].school_number =
                          [Success_2010].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2010].[dbo].[student_master]
                  ON [Success_2010].[dbo].[student_test].[student_id] =
                      [Success_2010].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2010].[dbo].[test_program]
                  ON [Success_2010].[dbo].[student_test].[test_prog_id] =
                                [Success_2010] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2010].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2010].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [Success_2011].[dbo].[student_test]
          INNER JOIN [Success_2011].[dbo]. [student_enrollment]
                  ON [Success_2011].[dbo].[student_test].[student_id] =
                                [Success_2011].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2011].[dbo].[student_test].school_year =
                          [Success_2011].[dbo]. [student_enrollment].school_year
                      AND [Success_2011].[dbo].[student_test].school_number =
                          [Success_2011].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2011].[dbo].[student_master]
                  ON [Success_2011].[dbo].[student_test].[student_id] =
                      [Success_2011].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2011].[dbo].[test_program]
                  ON [Success_2011].[dbo].[student_test].[test_prog_id] =
                                [Success_2011] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2011].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2011].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [Success_2012].[dbo].[student_test]
          INNER JOIN [Success_2012].[dbo]. [student_enrollment]
                  ON [Success_2012].[dbo].[student_test].[student_id] =
                                [Success_2012].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2012].[dbo].[student_test].school_year =
                          [Success_2012].[dbo]. [student_enrollment].school_year
                      AND [Success_2012].[dbo].[student_test].school_number =
                          [Success_2012].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2012].[dbo].[student_master]
                  ON [Success_2012].[dbo].[student_test].[student_id] =
                      [Success_2012].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2012].[dbo].[test_program]
                  ON [Success_2012].[dbo].[student_test].[test_prog_id] =
                                [Success_2012] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2012].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2012].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [test_program].test_program_desc
    FROM   [Success_2013].[dbo].[student_test]
          INNER JOIN [Success_2013].[dbo]. [student_enrollment]
                  ON [Success_2013].[dbo].[student_test].[student_id] =
                                [Success_2013].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2013].[dbo].[student_test].school_year =
                          [Success_2013].[dbo]. [student_enrollment].school_year
                      AND [Success_2013].[dbo].[student_test].school_number =
                          [Success_2013].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2013].[dbo].[student_master]
                  ON [Success_2013].[dbo].[student_test].[student_id] =
                      [Success_2013].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2013].[dbo].[test_program]
                  ON [Success_2013].[dbo].[student_test].[test_prog_id] =
                                [Success_2013] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2013].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2013].[dbo].[student_test].test_overall_score IS NOT
                          NULL
  `);
}

export function getCrtProficiency() {
  return msExecute(`
    SELECT 
       [student_test].school_year,
       [student_master].ssid,
       [student_enrollment].grade_level,
       [student_test].test_overall_score,
       [student_test].proficiency,
       [test_program].test_program_desc
    FROM   [sams_2008].[dbo].[student_test]
          INNER JOIN [sams_2008].[dbo].[student_enrollment]
                  ON [sams_2008].[dbo].[student_test].[student_id] =
                                [sams_2008].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2008].[dbo].[student_test].school_year =
                          [sams_2008].[dbo]. [student_enrollment].school_year
                      AND [sams_2008].[dbo].[student_test].school_number =
                          [sams_2008].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2008].[dbo].[student_master]
                  ON [sams_2008].[dbo].[student_test].[student_id] =
                      [sams_2008].[dbo] .[student_master].[student_id]
          INNER JOIN [sams_2008].[dbo].[test_program]
                  ON [sams_2008].[dbo].[student_test].[test_prog_id] =
                                [sams_2008] .[dbo].[test_program].[test_prog_id]
                      AND [sams_2008].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2008].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [sams_2009].[dbo].[student_test]
          INNER JOIN [sams_2009].[dbo]. [student_enrollment]
                  ON [sams_2009].[dbo].[student_test].[student_id] =
                                [sams_2009].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2009].[dbo].[student_test].school_year =
                          [sams_2009].[dbo]. [student_enrollment].school_year
                      AND [sams_2009].[dbo].[student_test].school_number =
                          [sams_2009].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2009].[dbo].[student_master]
                  ON [sams_2009].[dbo].[student_test].[student_id] =
                      [sams_2009].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_2009].[dbo].[test_program]
                  ON [sams_2009].[dbo].[student_test].[test_prog_id] =
                                [sams_2009] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_2009].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2009].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [sams_2010].[dbo].[student_test]
          INNER JOIN [sams_2010].[dbo].[student_enrollment]
                  ON [sams_2010].[dbo].[student_test].[student_id] =
                                [sams_2010].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2010].[dbo].[student_test].school_year =
                          [sams_2010].[dbo]. [student_enrollment].school_year
                      AND [sams_2010].[dbo].[student_test].school_number =
                          [sams_2010].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2010].[dbo].[student_master]
                  ON [sams_2010].[dbo].[student_test].[student_id] =
                      [sams_2010].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_2010].[dbo].[test_program]
                  ON [sams_2010].[dbo].[student_test].[test_prog_id] =
                                [sams_2010] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_2010].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2010].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [sams_2011].[dbo].[student_test]
          INNER JOIN [sams_2011].[dbo]. [student_enrollment]
                  ON [sams_2011].[dbo].[student_test].[student_id] =
                                [sams_2011].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2011].[dbo].[student_test].school_year =
                          [sams_2011].[dbo]. [student_enrollment].school_year
                      AND [sams_2011].[dbo].[student_test].school_number =
                          [sams_2011].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2011].[dbo].[student_master]
                  ON [sams_2011].[dbo].[student_test].[student_id] =
                      [sams_2011].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_2011].[dbo].[test_program]
                  ON [sams_2011].[dbo].[student_test].[test_prog_id] =
                                [sams_2011] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_2011].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2011].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [sams_2012].[dbo].[student_test]
          INNER JOIN [sams_2012].[dbo].[student_enrollment]
                  ON [sams_2012].[dbo].[student_test].[student_id] =
                                [sams_2012].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2012].[dbo].[student_test].school_year =
                          [sams_2012].[dbo]. [student_enrollment].school_year
                      AND [sams_2012].[dbo].[student_test].school_number =
                          [sams_2012].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2012].[dbo].[student_master]
                  ON [sams_2012].[dbo].[student_test].[student_id] =
                      [sams_2012].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_2012].[dbo].[test_program]
                  ON [sams_2012].[dbo].[student_test].[test_prog_id] =
                                [sams_2012] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_2012].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2012].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [sams_2013].[dbo].[student_test]
          INNER JOIN [sams_2013].[dbo]. [student_enrollment]
                  ON [sams_2013].[dbo].[student_test].[student_id] =
                                [sams_2013].[dbo]. [student_enrollment].[student_id]
                      AND [sams_2013].[dbo].[student_test].school_year =
                          [sams_2013].[dbo]. [student_enrollment].school_year
                      AND [sams_2013].[dbo].[student_test].school_number =
                          [sams_2013].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_2013].[dbo].[student_master]
                  ON [sams_2013].[dbo].[student_test].[student_id] =
                      [sams_2013].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_2013].[dbo].[test_program]
                  ON [sams_2013].[dbo].[student_test].[test_prog_id] =
                                [sams_2013] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_2013].[dbo].[student_test].test_overall_score != 0
                      AND [sams_2013].[dbo].[student_test].test_overall_score IS NOT
                          NULL
    UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [sams_merge].[dbo].[student_test]
          INNER JOIN [sams_merge].[dbo]. [student_enrollment]
                  ON [sams_merge].[dbo].[student_test].[student_id] =
                                [sams_merge].[dbo]. [student_enrollment].[student_id]
                      AND [sams_merge].[dbo].[student_test].school_year =
                          [sams_merge].[dbo]. [student_enrollment].school_year
                      AND [sams_merge].[dbo].[student_test].school_number =
                          [sams_merge].[dbo]. [student_enrollment].school_number
          INNER JOIN [sams_merge].[dbo].[student_master]
                  ON [sams_merge].[dbo].[student_test].[student_id] =
                      [sams_merge].[dbo] . [student_master].[student_id]
          INNER JOIN [sams_merge].[dbo].[test_program]
                  ON [sams_merge].[dbo].[student_test].[test_prog_id] =
                                [sams_merge] .[dbo]. [test_program].[test_prog_id]
                      AND [sams_merge].[dbo].[student_test].test_overall_score != 0
                      AND [sams_merge].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [Success_2008].[dbo].[student_test]
          INNER JOIN [Success_2008].[dbo]. [student_enrollment]
                  ON [Success_2008].[dbo].[student_test].[student_id] =
                                [Success_2008].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2008].[dbo].[student_test].school_year =
                          [Success_2008].[dbo]. [student_enrollment].school_year
                      AND [Success_2008].[dbo].[student_test].school_number =
                          [Success_2008].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2008].[dbo].[student_master]
                  ON [Success_2008].[dbo].[student_test].[student_id] =
                      [Success_2008].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2008].[dbo].[test_program]
                  ON [Success_2008].[dbo].[student_test].[test_prog_id] =
                                [Success_2008] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2008].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2008].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [Success_2009].[dbo].[student_test]
          INNER JOIN [Success_2009].[dbo]. [student_enrollment]
                  ON [Success_2009].[dbo].[student_test].[student_id] =
                                [Success_2009].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2009].[dbo].[student_test].school_year =
                          [Success_2009].[dbo]. [student_enrollment].school_year
                      AND [Success_2009].[dbo].[student_test].school_number =
                          [Success_2009].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2009].[dbo].[student_master]
                  ON [Success_2009].[dbo].[student_test].[student_id] =
                      [Success_2009].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2009].[dbo].[test_program]
                  ON [Success_2009].[dbo].[student_test].[test_prog_id] =
                                [Success_2009] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2009].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2009].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [Success_2010].[dbo].[student_test]
          INNER JOIN [Success_2010].[dbo]. [student_enrollment]
                  ON [Success_2010].[dbo].[student_test].[student_id] =
                                [Success_2010].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2010].[dbo].[student_test].school_year =
                          [Success_2010].[dbo]. [student_enrollment].school_year
                      AND [Success_2010].[dbo].[student_test].school_number =
                          [Success_2010].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2010].[dbo].[student_master]
                  ON [Success_2010].[dbo].[student_test].[student_id] =
                      [Success_2010].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2010].[dbo].[test_program]
                  ON [Success_2010].[dbo].[student_test].[test_prog_id] =
                                [Success_2010] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2010].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2010].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [Success_2011].[dbo].[student_test]
          INNER JOIN [Success_2011].[dbo]. [student_enrollment]
                  ON [Success_2011].[dbo].[student_test].[student_id] =
                                [Success_2011].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2011].[dbo].[student_test].school_year =
                          [Success_2011].[dbo]. [student_enrollment].school_year
                      AND [Success_2011].[dbo].[student_test].school_number =
                          [Success_2011].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2011].[dbo].[student_master]
                  ON [Success_2011].[dbo].[student_test].[student_id] =
                      [Success_2011].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2011].[dbo].[test_program]
                  ON [Success_2011].[dbo].[student_test].[test_prog_id] =
                                [Success_2011] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2011].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2011].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT 
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [Success_2012].[dbo].[student_test]
          INNER JOIN [Success_2012].[dbo]. [student_enrollment]
                  ON [Success_2012].[dbo].[student_test].[student_id] =
                                [Success_2012].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2012].[dbo].[student_test].school_year =
                          [Success_2012].[dbo]. [student_enrollment].school_year
                      AND [Success_2012].[dbo].[student_test].school_number =
                          [Success_2012].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2012].[dbo].[student_master]
                  ON [Success_2012].[dbo].[student_test].[student_id] =
                      [Success_2012].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2012].[dbo].[test_program]
                  ON [Success_2012].[dbo].[student_test].[test_prog_id] =
                                [Success_2012] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2012].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2012].[dbo].[student_test].test_overall_score IS NOT
                          NULL
UNION
    SELECT
          [student_test].school_year,
          [student_master].ssid,
          [student_enrollment].grade_level,
          [student_test].test_overall_score,
          [student_test].proficiency,
          [test_program].test_program_desc
    FROM   [Success_2013].[dbo].[student_test]
          INNER JOIN [Success_2013].[dbo]. [student_enrollment]
                  ON [Success_2013].[dbo].[student_test].[student_id] =
                                [Success_2013].[dbo]. [student_enrollment].[student_id]
                      AND [Success_2013].[dbo].[student_test].school_year =
                          [Success_2013].[dbo]. [student_enrollment].school_year
                      AND [Success_2013].[dbo].[student_test].school_number =
                          [Success_2013].[dbo]. [student_enrollment].school_number
          INNER JOIN [Success_2013].[dbo].[student_master]
                  ON [Success_2013].[dbo].[student_test].[student_id] =
                      [Success_2013].[dbo] . [student_master].[student_id]
          INNER JOIN [Success_2013].[dbo].[test_program]
                  ON [Success_2013].[dbo].[student_test].[test_prog_id] =
                                [Success_2013] .[dbo]. [test_program].[test_prog_id]
                      AND [Success_2013].[dbo].[student_test].test_overall_score != 0
                      AND [Success_2013].[dbo].[student_test].test_overall_score IS NOT
                          NULL
  `);
}
