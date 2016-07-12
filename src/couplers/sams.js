import {Observable} from '@reactivex/rxjs';
import {
  getCrtTestScores,
  getCrtProficiency,
  getCrtTestResultConcepts
} from '../service';
import { getTestResultsAndTestConcepts } from '../blogic';

export class SamsCoupler {
  constructor() {
    this.defaultName = 'sams';
  }

  /**
   * @returns {Observable}
   */
  from(channelName) {
    if (channelName === 'Test Result') {
      return getTestResultsAndTestConcepts();
    } else if (channelName === 'U_StudentTestProficiency') {
      return getCrtProficiency();
    } else if (channelName === 'Test Score') {
      return getCrtTestScores();
    }
  }
}
