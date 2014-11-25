var express = require('express');
var router = express.Router();

var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var client = new Client('localhost:2181');
var topics = [{topic: 'test', partition: 0}],
        options = { autoCommit: false,
            fromBeginning: false,
            fetchMaxWaitMs: 1000,
            fetchMaxBytes: 1024*1024
        }
    ;

function createConsumer(topics) {
    var consumer = new Consumer(client, topics, options);
    var offset = new Offset(client);
    consumer.on('message', function (message) {
        console.log(this.id, message);
    });
    consumer.on('error', function (err) {
        console.log('error', err);
    });
    consumer.on('offsetOutOfRange', function (topic) {
        topic.maxNum = 2;
        offset.fetch([topic], function (err, offsets) {
            var min = Math.min.apply(null, offsets[topic.topic][topic.partition]);
            consumer.setOffset(topic.topic, topic.partition, min);
        });
    })
}

/* GET users listing. */

router.get('/', function(req, res) {
  createConsumer(topics);
  res.render('stats.jade');
});

module.exports = router;