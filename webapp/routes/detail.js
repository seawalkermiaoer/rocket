var express = require('express');
var uuid = require('node-uuid')
// redis client
var redis   = require('redis');
var client  = redis.createClient('6379', '127.0.0.1');


// kafka client
var kafka = require('kafka-node'),
    Producer = kafka.Producer,
    Client = kafka.Client;
var kafka_client, producer;
connectKafka();

function connectKafka(){
    kafka_client = new Client();
    producer = new Producer(kafka_client);
        producer.on('ready', function () {
        console.log('connect kafka success!');
    });
    
    producer.on('error', function (err) {
        console.log('connect kafka fail!', err)
    });
}


function send(msg) {
    console.log(msg)
    var payloads = [
        { topic: 'test', messages: msg, partition: 0 }
    ];
    producer.send(payloads, function (err, data) {
        console.log(data);
    });
}


var router = express.Router();

router.get('/:id', function(req, res) {
	var id= req.params.id;
	var uid=req.cookies.uid;
	console.log(uid + ': ' + id)

    if(uid==null) {
        console.log("Unidentified User");
        uid = uuid.v4();
        res.cookie("uid", uid,  { expires: new Date(Date.now() + 9000000000)});
    }

	client.get(id, function(err, reply) {
		if(err!=null) {
			res.render('redis_error.jade', {title: "Error reading from redis"+err});
			return ;
		}
		if(reply==null) {
			res.render('not_found.jade', {title: "Producto not found"});
			return ;
		}
		var product = JSON.parse(reply);
		var current = new Date();
   		var entry = JSON.stringify({user: uid, action: "view",product: id, type: product.category, time:current.toGMTString()});
    	
    	console.log(entry);
    	send(entry)
		res.render('detail.jade', product);
	});

});

module.exports = router;
