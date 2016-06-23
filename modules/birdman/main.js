const Twitter = require('twitter');
const config = require('config');
const redisCommunicator = require('./communicators/redis');

const USER_TIMELINE_ENDPOINT = 'statuses/user_timeline';
const USER_STATUSES_STREAM_ENDPOINT = 'statuses/filter';
const TWITTER_FIRST_POSTS_CONFIG = {
  screen_name: config.get('TWITTER_USER_NAME_TO_FOLLOW')
};
const TWITTER_USER_TO_FOLLOW = {
  follow: config.get('TWITTER_USER_ID_TO_FOLLOW')
};

/**
 * Creates twitter node library client
 *
 * @returns {Twitter} - twitter client instance
 */
function createTwitterClient() {
  return new Twitter({
    consumer_key: config.get('TWITTER_CONSUMER_KEY'),
    consumer_secret: config.get('TWITTER_CONSUMER_SECRET'),
    access_token_key: config.get('TWITTER_ACCESS_TOKEN'),
    access_token_secret: config.get('TWITTER_ACCESS_TOKEN_SECRET')
  });
}

/**
 * Gets last posts for provided in config user
 *
 * @param client - twitter client instance
 * @returns {Promise} - resolved tweets
 */
function getLastTwitterPosts(client) {
  const twitterConfigs = Object.assign({}, TWITTER_FIRST_POSTS_CONFIG, {
    count: config.get('TWITTER_LAST_POSTS_COUNT')
  });

  return new Promise((resolve) => {
    client.get(USER_TIMELINE_ENDPOINT, twitterConfigs, (error, tweets) => {
      if (error) {
        throw new Error('Something went wrong with retrieving first posts', error);
      }

      resolve(tweets);
    });
  });
}

/**
 * Starts to listen to the stream and passes the tweets to redis communicator for save
 *
 * @param twitterClient - twitter client instance
 */
function setupTwitterStreamListener(twitterClient) {
  const stream = twitterClient.stream(USER_STATUSES_STREAM_ENDPOINT, TWITTER_USER_TO_FOLLOW);

  stream.on('data', (data) => {
    redisCommunicator.parse(data);
  });

  stream.on('error', (error) => {
    throw new Error(`Twitter error, ${error}`);
  });
}

/**
 * Inits the module.
 *
 * First inits the redis communicator.
 * Gets last twitter posts
 * Inits stream message listening
 */
function initBirdman() {
  const twitterClient = createTwitterClient();

  redisCommunicator.init().then(() => {
    getLastTwitterPosts(twitterClient).then((lastPosts) => {
      redisCommunicator.parse(lastPosts);
    });

    setupTwitterStreamListener(twitterClient);
  });
}

module.exports = initBirdman;
