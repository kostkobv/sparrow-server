/**
 * Pigeon-post is the module which handles the user connections to socket
 * and receive data from birdman to send it to the clients connected via socket
 */

const config = require('config');
const RSMQWorker = require('rsmq-worker');
const socketServer = require('./services/socket');


/**
 * Handles incoming message from worker
 *
 * @param message - received message body
 * @param next - next function
 * @param id - received message id
 * @param worker - worker
 */
function handleMessages(message, next, id, worker) {
  socketServer.broadcastNewTweet(message);
  worker.del(id);
  next();
}


/**
 * Inits worker for RSMQ to receive new tweets from birdman
 * @returns {*}
 */
function initWorker() {
  const worker = new RSMQWorker(config.get('RSMQ_QUEUE_NAME'), {
    host: config.get('REDIS_HOST'),
    port: config.get('REDIS_PORT'),
    autostart: true
  });

  return worker;
}

/**
 * Inits the pigeon-post module
 */
function init() {
  console.log('Init Pigeon Post');
  const worker = initWorker();

  socketServer.init();

  worker.on('message', (message, next, id) => handleMessages(message, next, id, worker));
}


module.exports = init;
