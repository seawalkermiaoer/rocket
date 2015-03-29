var express = require('express');
var router = express.Router();
//kafka
var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var client = new Client('localhost:2181');
var topics = [{topic: 'stats', partition: 0}],
    options = { 
            autoCommit: false,
            fromBeginning: false,
            fromOffest:false,
            fetchMaxWaitMs: 1000,
            fetchMaxBytes: 1024*1024
    };

function createConsumer(topics) {
    var consumer = new Consumer(client, topics, options);
    var offset = new Offset(client);

    consumer.on('message', function (message) {
        if (message.offset > 1199) {
            console.log(message.key.toString('ascii'))
            console.log(message.value)
        }
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


createConsumer(topics);

/* GET users listing. */

router.get('/', function(req, res) {
  //res.render('stats.jade');
  res.sendFile(__dirname + '/../views/index.html');
});

module.exports = router;