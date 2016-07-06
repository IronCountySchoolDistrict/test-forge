import {Observable} from '@reactivex/rxjs';
import {
  getCrtTestScores,
  getCrtProficiency,
  getCrtTestResults,
  getCrtTestResultConcepts
} from '.././service';

export class SamsCoupler {
  constructor() {
    this.defaultName = 'sams';
  }

  /**
   * @returns {Observable}
   */
  from(channelName) {
    if (channelName === 'Test Result') {
      return getCrtTestResults();
    } else if (channelName === 'U_StudentTestProficiency') {
      return getCrtProficiency();
    } else if (channelName === 'Test Score') {
      return getCrtTestScores();
    }
  }
}
