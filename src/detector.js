/**
 *  Detect what type of test (DIBELS, ACT, CRT, SAGE, etc) is getting passed in
 *  @param   {object} csvData
 *  @returns {string} test type
 */
export default function detect(csvData) {
  if (csvData['Assessment'] === 'mCLASS:DIBELS') {
    return {
      type: 'dibels',
      name: 'ROGL'
    };
  } else {
    if (csvData['student_test_id'] &&
      csvData['test_prog_id'] &&
      csvData['student_id']) {
        return {
          type: 'CRT',
          name: 'CRT'
        }
      }
    }
  }
