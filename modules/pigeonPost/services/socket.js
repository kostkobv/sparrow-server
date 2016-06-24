const Kalm = require('kalm');
const ws = require('kalm-websocket');
const redisService = require('./redis');
const config = require('config');

let kalmServer;

/**
 * Inits socket server
 */
function setServer() {
  Kalm.adapters.register('ws', ws);

  console.log(`Server is going to be up on ${config.get('SOCKET_SERVER_PORT')}`);

  return new Kalm.Server({
    port: config.get('SOCKET_SERVER_PORT'),
    adapter: 'ws',
    channels: {
      '/': () => {
        console.log(arguments);
      }
    }
  });
}

/**
 * Inits server and start to listen for the new connected clients to return the last posts list
 */
function init() {
  kalmServer = setServer();

  kalmServer.on('connection', function(client) {
    redisService.getLastPosts().then((tweets) => {
      client.send('tweets', tweets);
    });
  });

  console.log('Server is up');
}

/**
 * Broadcasts new tweet to all connected clients
 *
 * @param data - raw tweet data
 */
function broadcastNewTweet(data) {
  const parsedData = JSON.parse(data);
  const decodedData = redisService.decodeData(parsedData.data);

  kalmServer.broadcast('tweet', decodedData);
}

module.exports = {
  init,
  broadcastNewTweet
};