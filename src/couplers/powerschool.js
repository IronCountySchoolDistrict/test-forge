import {Observable} from '@reactivex/rxjs';
import {getCrtTestScores, createTestScore} from '.././service';

export class PowerSchoolCoupler {
  constructor(config) {
    this.defaultName = 'powerschool';
  }

  to(channelName, observable) {
    if (channelName === 'Test Scores') {
      return observable
        .subscribe(testScore => {
          createTestScore(testScore.testId, 'TODO: cut off name to 35 chars here', testScore.concept_desc);
        });
    }
  }
}