/**
 * This communicator is used for saving the data into the Redis and send the message via RSMQ 
 * to the PigeonPost
 */

const config = require('dotenv').config();

const redis = require('redis');
const redisClient = redis.createClient();

const RedisSMQ = require('rsmq');
const rsmq = new RedisSMQ({ host: config.REDIS_HOST, port: config.REDIS_PORT });
const rsmqConfigs = {
  qname: config.RSMQ_QUEUE_NAME
};

const pigeonPost = require('../../pigeonPost/main');

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
 */
function parse(tweets) {
  if (!Array.isArray(tweets)) {
    tweets = [ tweets ];
  }

  for (const tweet of tweets) {
    processTweet(tweet);
  }
}

/**
 * Processes tweet for saving. Checks if the instance already exists in redis.
 *
 * @param tweet - tweet that should be processed
 */
function processTweet(tweet) {
  const tweetKey = getTweetKey(tweet.id);
  console.log(JSON.stringify(tweet));

  redisClient.sismember(config.REDIS_TWEETS_SET, tweet.id, (error, reply) => {
    if (error) {
      throw new Error(`Something wrong with redis ${error}`);
    }

    if (!reply) {
      redisClient.sadd(config.REDIS_TWEETS_SET, tweet.id);
      return saveTweet(tweet);
    }

    redisClient.exists(tweetKey, (err, exists) => {
      if (err) {
        throw new Error(`Something wrong with redis ${err}`);
      }

      if (!exists) {
        saveTweet(tweet);
      }
    });
  });
}

/**
 * Saves the tweet to the redis
 *
 * @param tweet - a new tweet that should be saved
 */
function saveTweet(tweet) {
  const mappedTweet = mapTweet(tweet);
  const stringifyMappedTweet = JSON.stringify(mappedTweet);
  const tweetKey = getTweetKey(tweet.id);

  redisClient.set(tweetKey, stringifyMappedTweet);
  sendRsmqMessage(mappedTweet);
}

/**
 * Generates redis tweet record key based on id
 *
 * @param id - tweet id
 * @returns {string} - redis tweet record key
 */
function getTweetKey(id) {
  return `${config.REDIS_TWEET_PREFIX}${id}`;
}

/**
 * Sends message with new tweet via RSMQ
 *
 * @param mappedTweet - already mapped tweet
 */
function sendRsmqMessage(mappedTweet) {
  const messageConfig = Object.assign({}, rsmqConfigs, {
    message: JSON.stringify({
      data: mappedTweet
    })
  });

  rsmq.sendMessage(messageConfig, () => {});
}

module.exports = {
  parse
};