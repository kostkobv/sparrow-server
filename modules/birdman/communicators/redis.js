/**
 * This communicator is used for saving the data into the Redis and send the message via RSMQ 
 * to the PigeonPost
 */

const config = require('config');

const redis = require('redis');
const redisClient = redis.createClient({
  db: config.get('REDIS_DB')
});

const RedisSMQ = require('rsmq');
const rsmq = new RedisSMQ({ host: config.get('REDIS_HOST'), port: config.get('REDIS_PORT') });
const rsmqConfigs = {
  qname: config.get('RSMQ_QUEUE_NAME')
};

/**
 * Checks if queue for messaging exists. If not - creating a new queue
 *
 * @returns {Promise}
 */
function init() {
  return new Promise((resolve) => {
    rsmq.getQueueAttributes(rsmqConfigs, (err, response) => {
      if (response) {
        return resolve();
      }

      createRSMQQueue().then(() => {
        resolve();
      });
    });
  });
}

/**
 * Creates a new Queue for RSMQ
 *
 * @returns {Promise}
 */
function createRSMQQueue() {
  return new Promise((resolve, reject) => {
    rsmq.createQueue(rsmqConfigs, (error) => {
      if (error) {
        reject(error);
        throw new Error(`Couldn't create new Redis Queue, ${error}`);
      }

      resolve();
    });
  });
}

/**
 * Maps the tweet to redis-friendly format
 *
 * @param tweet - tweet message
 * @returns {*[]} - mapped tweet
 */
function mapTweet(tweet) {
  return [
    'id', tweet.id,
    'text', tweet.text,
    'entities', tweet.entities,
    'user:name', tweet.user.name,
    'user:url', tweet.user.url,
    'user:profileImageUrlHttps', tweet.user.profile_image_url_https
  ];
}

/**
 * Parses the received tweet(s) and passing it forward for saving and etc
 *
 * @param tweets - the tweet collection (can be just single tweet object)
 * @returns {Promise}
 */
function parse(tweets) {
  if (!Array.isArray(tweets)) {
    tweets = [ tweets ];
  }

  const promises = [];

  for (const tweet of tweets) {
    const promise = processTweet(tweet);
    promises.push(promise);
  }

  return Promise.all(promises);
}

/**
 * Adds tweet id to the set with tweets
 *
 * @param tweetsSet - tweets set key
 * @param tweet - tweet that should be added
 * @returns {Promise}
 */
function addTweetIdToTweetsSet(tweetsSet, tweet) {
  return new Promise((resolve) => {
    redisClient.sadd(tweetsSet, tweet.id, (err) => {
      if (err) {
        throw new Error(err);
      }

      resolve();
    });
  });
}

/**
 * Processes tweet for saving. Checks if the instance already exists in redis.
 *
 * @param tweet - tweet that should be processed
 * @returns {Promise}
 */
function processTweet(tweet) {
  return new Promise((resolve, reject) => {
    const tweetKey = getTweetKey(tweet.id);
    const tweetsSet = config.get('REDIS_TWEETS_SET');

    if (tweet.delete) {
      resolve();
    }

    redisClient.sismember(tweetsSet, tweet.id, (error, member) => {
      if (error) {
        reject(error);
      }

      if (!member) {
        addTweetIdToTweetsSet(tweetsSet, tweet);
      }

      checkIfTweetExists(tweetKey).then(resolve,
        () => {
          saveTweet(tweet).then(resolve, reject);
        }
      );
    });
  });
}

/**
 * Checks if tweet exists in db
 *
 * @param tweetKey - key that should be checked
 * @returns {Promise}
 */
function checkIfTweetExists(tweetKey) {
  return new Promise((resolve, reject) => {
    redisClient.exists(tweetKey, (err, exists) => {
      if (err) {
        reject(err);
      }

      if (!exists) {
        reject();
      }

      resolve();
    });
  });
}

/**
 * Saves the tweet to the redis and sends message to the RQMS queue
 *
 * @param tweet - a new tweet that should be saved
 * @returns {Promise}
 */
function saveTweet(tweet) {
  return new Promise((resolve, reject) => {
    const mappedTweet = mapTweet(tweet);
    const stringifyMappedTweet = JSON.stringify(mappedTweet);
    const tweetKey = getTweetKey(tweet.id);

    redisClient.set(tweetKey, stringifyMappedTweet, (err) => {
      if (err) {
        reject(err);
      }

      sendRsmqMessage(mappedTweet).then(() => {
        resolve();
      });
    });
  });
}

/**
 * Generates redis tweet record key based on id
 *
 * @param id - tweet id
 * @returns {string} - redis tweet record key
 */
function getTweetKey(id) {
  return `${config.get('REDIS_TWEET_PREFIX')}${id}`;
}

/**
 * Sends message with new tweet via RSMQ
 *
 * @param mappedTweet - already mapped tweet
 * @returns {Promise}
 */
function sendRsmqMessage(mappedTweet) {
  return new Promise((resolve, reject) => {
    const messageConfig = createMessageConfig(mappedTweet);

    rsmq.sendMessage(messageConfig, (err) => {
      if (err) {
        reject(err);
      }

      resolve();
    });
  });
}

/**
 * Creates a message configs (message body as well) that would be sent
 *
 * @param mappedTweet - mapped tweet body
 * @returns {*}
 */
function createMessageConfig(mappedTweet) {
  return Object.assign({}, rsmqConfigs, {
    message: JSON.stringify({
      data: mappedTweet
    })
  });
}

module.exports = {
  parse,
  init
};