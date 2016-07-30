var Twit = require('twit');
var MongoClient = require('mongodb').MongoClient;
var Mixpanel = require('mixpanel');

var mixpanel = Mixpanel.init('853752b7e48eb5a57a4b5ce6e3e17f95');
var pipeline = Mixpanel.init('853752b7e48eb5a57a4b5ce6e3e17f95', {
  host: '10.252.67.42',
  port: 8080,
});

var T = new Twit({
  consumer_key:         '39febZ09bcMJuwUUCJWBNGi2m',
  consumer_secret:      '2MjgH85yZOrlr8f8C2TXYiPNWT5dr4nYKINFZe4xKV0shzoAwy',
  access_token:         '223778263-h2Sx87E1tVEYP3uu64eSu2hkpKMBruKY8NWXr209',
  access_token_secret:  'pibJWSfnPfSo8kKQ71WR34VeSpTT2RFY8P9Uw3N4ykirc',
  timeout_ms:           60*1000,  // optional HTTP request timeout to apply to all requests.
});

MongoClient.connect('mongodb://10.252.67.42:27017/pipeline', function(err, db) {
  process.stdout.write("Connected correctly to server");

  var eventCollection = db.collection('event');
  var userCollection = db.collection('user');

  function insert(keyword) {
    return function callback(tweet) {
      tweet['keyword'] = keyword;
      tweet['timestamp'] = new Date(parseInt(tweet['timestamp_ms']));
      tweet['distinct_id'] = tweet['user']['id'];
      tweet['user']['_id'] = tweet['user']['id'];

      eventCollection.insert(tweet);
      userCollection.save(tweet['user']);

      mixpanel.track('twit', tweet);
      mixpanel.people.set(tweet['user']['id'], tweet['user']);

      pipeline.track('twit', tweet);
      pipeline.people.set(tweet['user']['id'], tweet['user']);

      process.stdout.write('.');
    };
  }

  T
  .stream('statuses/filter', { track: 'cloud' })
  .on('tweet', insert('cloud'));

  T
  .stream('statuses/filter', { track: 'data' })
  .on('tweet', insert('data'));

  // var sanFrancisco = [ '-122.75', '36.8', '-121.75', '37.8' ]
  // var stream = T.stream('statuses/filter', { locations: sanFrancisco })
  // stream.on('tweet', function (tweet) {
  //   collection.insert(tweet);
  // })

  // var stream = T.stream('statuses/filter', { track: '#docker', language: 'en' })
  // stream.on('tweet', function (tweet) {
  //   collection.insert(tweet);
  // })

  // var stream = T.stream('statuses/sample')
  // stream.on('tweet', function (tweet) {
  //   collection.insert(tweet);
  //   process.stdout.write('.');
  // })
});
