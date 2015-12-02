// Collection of database queries
import {
  execute
}
from './database';
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
    select id, name from test where name like '%'||:searchTerm||'%'
    `, [searchTerm], {
    outFormat: orawrap.OBJECT
  });
}

export function getStudentId(studentPrimaryId) {
  return execute(`
    select id from students where student_number=:studentPrimaryId
  `, [studentPrimaryId], {
    outFormat: orawrap.OBJECT
  });
}
