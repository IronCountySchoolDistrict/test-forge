/**
 *  Detect what type of test (DIBELS, ACT, CRT, SAGE, etc) is getting passed in
 *  @param   {object} csvData
 *  @returns {object} test type {
 *    type: "test type label",
 *    name: "PS.Test.Name"
 *  }
 */
export default function detect(csvData) {
  if (csvData['Assessment'] &&
    csvData['Assessment'] === 'mCLASS:DIBELS') {
    return {
      type: 'dibels',
      name: 'ROGL'
    };
  }
}
