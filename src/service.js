// Collection of database queries
import {
  execute
}
from './database';
import orawrap from 'orawrap';

export function getStudentTests(studentNumber, termName, testid) {
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
              `, [studentNumber, termName], {
    outFormat: orawrap.OBJECT
  })
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

export function getSchoolId(schoolName) {
  return execute(`
    select school_number from schools where name like '%'||:schoolName||'%'
  `, [schoolName], {
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
