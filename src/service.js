// Collection of database queries
import {
  execute,
  msExecute
}
from './database';
import Promise from 'bluebird';
import orawrap from 'orawrap';

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

export function getMatchingStudentTestScore(studentNumber, termName, alphaScore, testId) {
  return execute(`
    SELECT studenttestscore.dcid
    FROM studenttestscore
      JOIN studenttest ON studenttest.id = STUDENTTESTSCORE.STUDENTTESTID
      JOIN students on studenttest.STUDENTID=students.id
    WHERE students.student_number = :student_number
          AND studenttestscore.alphascore=:alpha_score
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

/**
 * return all tests that match the searchTerm
 * @param  {[type]} searchTerm [description]
 * @return {[type]}            [description]
 */
export function getMatchingTests(searchTerm) {
  return execute(`
    SELECT id, name
    FROM test
    WHERE name LIKE '%'||:searchTerm||'%'
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

export function getStudentId(studentPrimaryId) {
  return execute(`
    SELECT id
    FROM students
    WHERE student_number=:studentPrimaryId
  `, [studentPrimaryId], {
    outFormat: orawrap.OBJECT
  });
}

export function getStudentIdFromSsid(ssid) {
  console.log('in getStudentIdFromSsid');
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
  console.log('in getStudentNumberFromSsid');
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
    FROM [student_test]
      INNER JOIN [student_enrollment]
        ON [student_test].[student_id] = [student_enrollment].[student_id]
           AND [student_test].school_year = [student_enrollment].school_year
           AND [student_test].school_number = [student_enrollment].school_number
      INNER JOIN [student_master]
        ON [student_test].[student_id] = [student_master].[student_id]
      INNER JOIN [test_program]
        ON [student_test].[test_prog_id] = [test_program].[test_prog_id]
        AND [student_test].test_overall_score != 0
        AND [student_test].test_overall_score is not null
      WHERE [student_master].ssid=1603762
      ORDER BY student_test.school_year DESC
  `);
}
