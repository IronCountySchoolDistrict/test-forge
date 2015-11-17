import through from 'through';
import { getStudentTests } from './service';

export default function transform(testId) {
  function write(data) {
    getStudentTests(data['Student Primary ID'], data['School Year'], testId)
      .then(studentTests => {
        console.log(studentTests.rows);
        console.log(typeof this);
        this.emit('data', studentTests.rows[0]);
      });
  }

  function end() {
    this.emit('end');
  }
  return through(write, end);
}
