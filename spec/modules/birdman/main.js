const config = require('config');

const redis = require('redis');
const redisClient = redis.createClient({
  db: config.get('REDIS_DB')
});

const RSMQWorker = require('rsmq-worker');

const test = require('tape');

const redisCommunicator = require('../../../modules/birdman/communicators/redis');

redisClient.flushall();

redisCommunicator.init().then(() => {
  const tweet = {
    created_at: "Tue Jun 21 18:29:14 +0000 2016",
    id: 745322704471887900,
    id_str: "745322704471887873",
    text: "From bird flu to malaria: open access journal takes holistic approach to disease control: One Health focuses ... https://t.co/kiHqSQl8Kf",
    truncated: false,
    entities: {
      hashtags: [],
      symbols: [],
      user_mentions: [],
      urls: [
        {
          url: "https://t.co/kiHqSQl8Kf",
          expanded_url: "http://bit.ly/28MYUV2",
          display_url: "bit.ly/28MYUV2",
          indices: [113, 136]
        }
      ]
    },
    source: "<a href=\"http://twitterfeed.com\" rel=\"nofollow\">twitterfeed</a>",
    in_reply_to_status_id: null,
    in_reply_to_status_id_str: null,
    in_reply_to_user_id: null,
    in_reply_to_user_id_str: null,
    in_reply_to_screen_name: null,
    user: {
      id: 50998548,
      id_str: "50998548",
      name: "Elsevier News",
      screen_name: "ElsevierNews",
      location: "Amsterdam",
      description: "Press releases and other news from Elsevier. Get in touch at newsroom@elsevier.com.",
      url: "http://t.co/U73ua5NnQs",
      entities: {
        url: {
          urls: [
            {
              url: "http://t.co/U73ua5NnQs",
              expanded_url: "http://www.elsevier.com/about/press-releases",
              display_url: "elsevier.com/about/press-re…",
              indices: [0, 22]
            }
          ]
        },
        description: {
          urls: []
        }
      },
      protected: false,
      followers_count: 17829,
      friends_count: 351,
      listed_count: 394,
      created_at: "Fri Jun 26 09:58:48 +0000 2009",
      favourites_count: 23,
      utc_offset: 7200,
      time_zone: "Amsterdam",
      geo_enabled: false,
      verified: false,
      statuses_count: 3296,
      lang: "en",
      contributors_enabled: false,
      is_translator: false,
      is_translation_enabled: false,
      profile_background_color: "C0DEED",
      profile_background_image_url: "http://abs.twimg.com/images/themes/theme1/bg.png",
      profile_background_image_url_https: "https://abs.twimg.com/images/themes/theme1/bg.png",
      profile_background_tile: false,
      profile_image_url: "http://pbs.twimg.com/profile_images/715515784194338817/FzWwrJdY_normal.jpg",
      profile_image_url_https: "https://pbs.twimg.com/profile_images/715515784194338817/FzWwrJdY_normal.jpg",
      profile_banner_url: "https://pbs.twimg.com/profile_banners/50998548/1459428312",
      profile_link_color: "007398",
      profile_sidebar_border_color: "FFFFFF",
      profile_sidebar_fill_color: "DDEEF6",
      profile_text_color: "333333",
      profile_use_background_image: true,
      has_extended_profile: false,
      default_profile: false,
      default_profile_image: false,
      following: null,
      follow_request_sent: null,
      notifications: null
    },
    geo: null,
    coordinates: null,
    place: null,
    contributors: null,
    is_quote_status: false,
    retweet_count: 1,
    favorite_count: 4,
    favorited: false,
    retweeted: false,
    possibly_sensitive: false,
    lang: "en"
  };

  test('Redis communicator should save provided tweet into redis', (assert) => {
    redisCommunicator.parse(tweet).then(() => {
      redisClient.get(`${config.get('REDIS_TWEET_PREFIX')}${tweet.id}`, (err, response) => {
        assert.error(err, 'Something wrong with redis');

        const parsedResponse = JSON.parse(response);

        assert.equal(parsedResponse[0], 'id');
        assert.equal(parsedResponse[1], tweet.id);

        redisClient.end(true);

        assert.end();
      });
    });
  });

  test('Save should trigger the RSMQ message', (assert) => {
    const worker = new RSMQWorker(config.get('RSMQ_QUEUE_NAME'), {
      host: config.get('REDIS_HOST'),
      port: config.get('REDIS_PORT')
    });

    worker.on('message', (message, next, id) => {
      const data = JSON.parse(message).data;

      assert.equal(data[1], tweet.id);
      assert.end();
      worker.del(id);
      next();
      worker.quit();
    });

    worker.start();
  });

  test('Should not emit the message if tweet already exists', (assert) => {
    redisCommunicator.parse(tweet);

    const worker = new RSMQWorker(config.get('RSMQ_QUEUE_NAME'), {
      host: config.get('REDIS_HOST'),
      port: config.get('REDIS_PORT')
    });

    worker.on('message', (message, next, id) => {
      assert.fail('Message came.');
      worker.del(id);
      next();
      worker.quit();
    });

    setTimeout(() => {
      assert.end();
    }, 500);

    worker.start();
  });

  test.onFinish(() => {
    process.exit();
  });
});