var express = require('express');
var router = express.Router();
var redis = require("redis")
var client = redis.createClient()

/**
 * Add the FIFO functionality to Array class.
 **/
Array.prototype.store = function (info) {
  this.push(info);
}

Array.prototype.fetch = function() {
  if (this.length <= 0) { return ''; }  // nothing in array
  return this.shift();
}

Array.prototype.display = function() {
  return this.join('\n');
}


function populate_redis() {
	var test_products = [
		{title:"Dvd player with surround sound system", category:"Players", price: 100},
		{title:"Full HD Bluray and DVD player", category:"Players", price:130},
		{title:"Media player with USB 2.0 input", category:"Players", price:70},

		{title:"Full HD Camera", category:"Cameras", price:200},
		{title:"Waterproof HD Camera", category:"Cameras", price:300},
		{title:"ShockProof and Waterproof HD Camera", category:"Cameras", price:400},
		{title:"Reflex Camera", category:"Cameras", price:500},

		{title:"DualCore Android Smartphon with 64Gb SD card", category:"Phones", price:200},
		{title:"Regular Movile Phone", category:"Phones", price:20},
		{title:"Satellite phone", category:"Cameras", price:500},

		{title:"64Gb SD Card", category:"Memory", price:35},
		{title:"32Gb SD Card", category:"Memory", price:27},
		{title:"16Gb SD Card", category:"Memory", price:5},

		{title:"Pink smartphone cover", category:"Covers", price:20},
		{title:"Black smartphone cover", category:"Covers", price:20},
		{title:"Kids smartphone cover", category:"Covers", price:30},

		{title:"55 Inches LED TV", category:"TVs", price:800},
		{title:"50 Inches LED TV", category:"TVs", price:700},
		{title:"42 Inches LED TV", category:"TVs", price:600},
		{title:"32 Inches LED TV", category:"TVs", price:400},

		{title:"TV Wall mount bracket 32-42 Inches", category:"Mounts", price:50},
		{title:"TV Wall mount bracket 50-55 Inches", category:"Mounts", price:80}
	];

	for(var i=0; i < test_products.length ;i++) {
		var product = test_products[i];
		product['id'] = i;
		var str = JSON.stringify(product);
		console.log("Inserting test product: [" + i  +"]:"+str);
		client.set(i, str);
		client.sadd("categories", product.category);
		client.sadd(product.category, product.id);
		client.sadd("products", product.id);
	}
}

populate_redis();

function get_all_data(fn) {
	var products = {};
    client.smembers('products', function(err, resp){
		var ids = resp;
		var readed = 0;
		for(var i = 0 ; i < ids.length ; i++) {
			client.get(ids[i], function(err, resp) {
				products[ids[readed]] = JSON.parse(resp);
				readed ++;
				if(readed == ids.length){
					fn(products);
				}
			});
		}
    });
}


/* GET home page. */
router.get('/', function(req, res) {
	get_all_data(function(products){
		var data = {title: "home page", products:products};
		console.log(products)
		res.render('index.jade', data);
	});
});

module.exports = router;
