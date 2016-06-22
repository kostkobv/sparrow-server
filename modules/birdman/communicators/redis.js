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

function init() {
  return new Promise((resolve) => {
    rsmq.getQueueAttributes(rsmqConfigs, (err, response) => {
      if (response) {
        return resolve();
      }

      rsmq.createQueue(rsmqConfigs, (error) => {
        if (error) {
          throw new Error(`Couldn't create new Redis Queue, ${error}`);
        }

        resolve();
      });
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
 * Processes tweet for saving. Checks if the instance already exists in redis.
 *
 * @param tweet - tweet that should be processed
 */
function processTweet(tweet) {
  return new Promise((resolve, reject) => {
    const tweetKey = getTweetKey(tweet.id);
    const tweetsSet = config.get('REDIS_TWEETS_SET');

    redisClient.sismember(tweetsSet, tweet.id, (error, reply) => {
      if (error) {
        reject(error);
        throw new Error(error);
      }

      if (!reply) {
        redisClient.sadd(tweetsSet, tweet.id, (err) => {
          if (err) {
            throw new Error(err);
          }

          return saveTweet(tweet).then(() => {
            resolve();
          });
        });

      }

      redisClient.exists(tweetKey, (err, exists) => {
        if (err) {
          reject(err);
          throw new Error(err);
        }

        if (!exists) {
          return saveTweet(tweet).then(() => {
            resolve();
          });
        }

        resolve();
      });

    });
  });
}

/**
 * Saves the tweet to the redis
 *
 * @param tweet - a new tweet that should be saved
 */
function saveTweet(tweet) {
  return new Promise((resolve, reject) => {
    const mappedTweet = mapTweet(tweet);
    const stringifyMappedTweet = JSON.stringify(mappedTweet);
    const tweetKey = getTweetKey(tweet.id);

    redisClient.set(tweetKey, stringifyMappedTweet, (err, val) => {
      if (err) {
        reject(err);
        throw new Error(err);
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
 */
function sendRsmqMessage(mappedTweet) {
  return new Promise((resolve, reject) => {
    const messageConfig = Object.assign({}, rsmqConfigs, {
      message: JSON.stringify({
        data: mappedTweet
      })
    });

    rsmq.sendMessage(messageConfig, (err) => {
      if (err) {
        reject(err);
        throw new Error(err);
      }

      resolve();
    });
  });
}

module.exports = {
  parse,
  init,
  rsmq
};