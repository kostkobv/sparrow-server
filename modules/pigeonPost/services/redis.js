/**
 * Service to handle data retrieving from Redis
 */

const config = require('config');

const redis = require('redis');
const redisClient = redis.createClient({
  db: config.get('REDIS_DB')
});

const REDIS_TWEETS_SET = `${config.get('REDIS_TWEETS_SET')}:${config.get('TWITTER_USER_ID_TO_FOLLOW')}`;

/**
 * Retrieves last count of posts according to configs (TWITTER_LAST_POSTS_COUNT)
 *
 * @returns {Promise} - promise with results
 */
function getLastPosts() {
  return new Promise((resolve) => {
    redisClient.sort(REDIS_TWEETS_SET, 'by', '*', 'get',
      `${config.get('REDIS_TWEET_PREFIX')}*`, 'desc', 'limit', '0',
      config.get('TWITTER_LAST_POSTS_COUNT'), (err, rawResult) => {
        if (err) {
          throw new Error(`${err}`);
        }

        const result = parseRedisResponse(rawResult);

        resolve(result);
      });
  });
}

/**
 * Parses data from redis stringified redis "object" format to regular collection
 *
 * Redis "object" example is:
 * ['id', 78293749237472398, 'text', 'textjslkjsdf', 'user:name', 'Name Name']
 *
 * @param rawResults - array with raw redis results collection
 * @returns {Array} - parsed redis results collection
 */
function parseRedisResponse(rawResults) {
  const results = [];

  for (const rawResult of rawResults) {
    const result = JSON.parse(rawResult);
    const decodedResult = decodeData(result);

    results.push(decodedResult);
  }

  return results;
}

/**
 * Generator for redis raw "object" which yields object with { key: value } for each key-value pair
 *
 * Check parseRedisResponse for redis raw "object" example
 *
 * @param data - raw redis object
 */
function *decodeDataGenerator(data) {
  for (const param in data) {
    const paramInt = parseInt(param, 10);

    if (paramInt % 2 === 0) {
      yield { [data[paramInt]]: data[paramInt + 1]};
    }
  }
}

/**
 * Transforms raw redis "object" to regular object
 *
 * Check parseRedisResponse docs for raw redis "object" example
 *
 * @param data - raw redis "object" that should be decoded
 * @returns {{}} - decoded object
 */
function decodeData(data) {
  const decodedData = {};

  for (const param of decodeDataGenerator(data)) {
    Object.assign(decodedData, param);
  }

  return decodedData;
}

module.exports = {
  getLastPosts,
  decodeData
};