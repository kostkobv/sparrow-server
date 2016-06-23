const commander = require('commander');
const birdman = require('./modules/birdman/main');
const pigeonPost = require('./modules/pigeonPost/main');

const modules = {
  birdman,
  'pigeon-post': pigeonPost
};

commander
  .version('1.0.0', null)
  .description('Sparrow');

commander
  .command('module <module-name>')
  .description('run module')
  .action((moduleName) => {
    try {
      modules[moduleName]();
    } catch (error) {
      throw new Error(error);
    }
  });

commander.parse(process.argv);

// Show help if no command provided
if (process.argv.length < 3) {
  commander.outputHelp();
}
