import {Observable} from '@reactivex/rxjs';
import {EOL} from 'os';
import {createWriteStream, truncateSync} from 'fs';
import json2csv from 'json2csv';
import Promise from 'bluebird';
import {property} from 'lodash';

var toCSV = Promise.promisify(json2csv);

export class CsvCoupler {

  constructor() {
    this.defaultName = 'csv';
  }

  /**
   * converts an Observable that emits objects to an Observable that emits csv strings
   *
   * @param config {object}
   * @param srcObservable {Observable}
   * @returns {Observable}
   */
  toCsvObservable(config, srcObservable) {
    return srcObservable.concatMap(function (item, i) {
      if (i === 0) {
        let outputFilename = `output/crt/EOL - ${property(config.groupByProperty)(item)}-${config.prompt.table}.txt`;

        // creates file if it doesn't exist
        var ws = createWriteStream(outputFilename, {
          flags: 'a'
        });

        try {
          ws.on('open', function (fd) {
            truncateSync(outputFilename);
          });
        } catch (e) {
          console.log('couldn\'t truncate file');
          console.log('e == %j', e);
        }
      }
      let csvOutputObj;
      if (!config.outputProperty) {
        csvOutputObj = item;
      } else {
        csvOutputObj = property(config.outputProperty)(item);
      }
      return toCSV({
        data: csvOutputObj,
        del: '\t',
        hasCSVColumnTitle: i === 0 // print columns only if this is the first item emitted
      })
        .then(csvStr => {
          let csvRemQuotes = csvStr.replace(/"/g, '');

          // Add a newline character before every line except the first line
          let csvVal = i === 0 ? csvRemQuotes : EOL + csvRemQuotes;
          if (ws) {
            return {
              csv: csvVal,
              ws: ws
            };
          } else {
            return {
              csv: csvVal
            };
          }
        });

    });
  }

  /**
   *
   * @param observable {Observable}
   * @param channel {string} path to the property of the emitted item object that should be used to group items by
   * @returns {Observable}
   */
  to(channel, observable) {
    return observable
      .groupBy(x => property(channel.groupByProperty)(x))
      .subscribe(groupedObservable => {
        let ws;
        this.toCsvObservable(channel, groupedObservable).subscribe(
          item => {
            if (!ws && item.ws) {
              ws = item.ws;
            }
            ws.write(item.csv);
          },

          error => console.log('error == ', error),

          () => {
            console.log('close writestream');
            ws.end();
          }
        );
      });
  }
}
