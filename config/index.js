import bunyan from 'bunyan';
import fs from 'fs';
import PrettyStream from 'bunyan-prettystream';
var prettyStdOut = new PrettyStream({mode: 'dev'});
prettyStdOut.pipe(process.stdout);

var prettyFileOut = new PrettyStream({mode: 'dev', useColor: false});
prettyFileOut.pipe(fs.createWriteStream('reindex.log'));

var defaultConfig = {
  log: bunyan.createLogger({
    name:    'es-transfer',
    streams: [
      {
        type:   'raw',
        level:  'info',
        stream: prettyStdOut
      },
      {
        type:   'raw',
        level:  'info',
        stream: prettyFileOut
      }
    ]
  })
};

export default defaultConfig;