var express = require('express');
var router = express.Router();

var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.Client();





/* GET users listing. */

router.get('/', function(req, res) {

  var consumer = new Consumer(client,
      [{ topic: 'test', partition: 0 }],
      {autoCommit: false}
  );

  consumer.on('message', function (message) {
      console.log(message);
  });

  res.render('stats.jade');
});

module.exports = router;