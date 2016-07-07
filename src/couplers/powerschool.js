import {Observable} from '@reactivex/rxjs';
import {getCrtTestScores, createTestScore} from '.././service';

export class PowerSchoolCoupler {
  constructor(config) {
    this.defaultName = 'powerschool';
  }

  to(channelName, observable) {
    if (channelName === 'Test Score') {
      return observable.subscribe(testScore => {
          createTestScore(testScore.testId, testScore.scoreName, testScore.concept_desc);
        });
    }
  }
}
