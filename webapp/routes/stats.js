var express = require('express');
var router = express.Router();

/* GET users listing. */

router.get('/', function(req, res) {
  res.send('respond with a stats from redis');

  client.get("foo", function(err, reply){
      console.log(reply.toString());
  });
});


// redis client
var redis   = require('redis');
var client  = redis.createClient('6379', '127.0.0.1');
module.exports = router;
