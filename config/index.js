import bunyan from 'bunyan';
import PrettyStream from 'bunyan-prettystream';
var prettyStdOut = new PrettyStream({mode: 'dev'});
prettyStdOut.pipe(process.stdout);

var defaultConfig = {
  log: bunyan.createLogger({
    name:    'es-transfer',
    streams: [
      {
        type:   'raw',
        level:  'info',
        stream: prettyStdOut
      }
    ]
  })
};

export default defaultConfig;