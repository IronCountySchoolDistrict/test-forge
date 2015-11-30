/**
 *  Detect what type of test (DIBELS, ACT, CRT, SAGE, etc) is getting passed in
 *  @param   {object} csvData
 *  @returns {string} test type
 */
export default function detect(csvData) {
  if (csvData['Assessment'] === 'mCLASS:DIBELS') {
    return 'dibels';
  }
}
