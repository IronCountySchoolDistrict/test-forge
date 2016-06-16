import {Observable} from '@reactivex/rxjs';
import {
  getCrtTestScores,
  getCrtProficiency,
  getCrtTestResults
} from '.././service';

export class SamsCoupler {
  constructor() {
    this.defaultName = 'sams';
  }

  /**
   * @returns {Observable}
   */
  from(channelName) {
    if (channelName === 'Test Results') {
      return getCrtTestResults();
    } else if (channelName === 'U_StudentTestProficiency') {
      return getCrtProficiency();
    } else if (channelName === 'Test Scores') {
      return getCrtTestScores();
    }
  }
}
