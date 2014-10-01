/**
 * Simple service to bring down the tweets to the locals if they're matched as relevant
 *
 * Usage : new Parser(db, { ... }).start();
 *
 * For possible options, see constructor below.
 */

var Twit = require('twit');

//------------------------------------------------------------------------------

function Parser(mongoClient, options)
{
	this.db = mongoClient;

	this.options = {
		creds : {	// credentials for twitter API
			consumer_key: '',
			consumer_secret: '',
			access_token: '',
			access_token_secret: '',
		},
		match : '',	// string to search tweet stream for
		state_collection : 'worker_state',
		state_id : 'last_tweet',
		storage_collection : 'tweets',
		account_blacklist : [],
		check_interval : 15000,
		batch_size : 100,
	};

	for (var i in options) {
		this.options[i] = options[i];
	}

	this.twit = new Twit(this.options.creds);
}

Parser.prototype.start = function(db)
{
	var self = this;

	this.db.collection(this.options.state_collection).find({_id : this.options.state_id}, function(err, res) {
		if (err) throw err;

		res.toArray(function(err, res) {
			if (err) throw err;

			var lastOffset = res.length ? res[0].value : 0;

			setImmediate(function() {
				self.loadTweets(lastOffset);
			});
		});
	});
};

Parser.prototype.loadTweets = function(lastOffset)
{
	var db = this.db;
	var options = this.options;
	var twit = this.twit;
	var self = this;

	console.log('Check tweets [offset ' + lastOffset + ']...');

	function parseTwitterDate(text)
	{
		return new Date(Date.parse(text.replace(/( +)/, ' UTC$1')));
	}

	twit.get('search/tweets', { q: options.match, count: this.options.batch_size, since_id: (lastOffset + 1) }, function(err, data, response) {
    	if (err) throw err;

    	console.log('Found ' + data.statuses.length + '.');

    	var coll = db.collection(options.storage_collection);
    	var maxId = lastOffset;
    	var writesInProgress = 0;

    	if (!data.statuses.length) {
    		self.finaliseRun(db, maxId);
    	} else {
	    	// check and find last ID
	    	data.statuses.forEach(function(status) {
    			if (status.id_str > maxId) {
    				maxId = status.id_str;
    			}
	    	});

    		// save individually as we sometimes get the same tweet ID in different result sets (possibly due to updates, but unsure)
	    	data.statuses.forEach(function(status) {
    		(function() {
    			++writesInProgress;
	    		var u = status.user;

	    		// check user blacklist
	    		if (options.account_blacklist.indexOf(u.screen_name) != -1) {
	    			return;
	    		}


	    		var tweet = {
            _id : status.id_str,
	    			id_str: status.id_str,
            created_at : parseTwitterDate(status.created_at),
            text : status.text,
	    			user : {
	    				id : u.id,
	    				name : u.name,
	    				screen_name : u.screen_name,
	    				location : u.location,
	    				description : u.description,
	    				url : u.url,
	    				followers_count : u.followers_count,
	    				friends_count : u.friends_count,
	    				time_zone : u.time_zone,
	    				profile_image_url : u.profile_image_url.replace(/^https?:/i, ''),
	    			}
	    		};


	    		coll.save(tweet, {w : 1}, function(err, res) {
		    		if (err) throw err;

		    		if (--writesInProgress == 0) {
			    		self.finaliseRun(db, maxId);
			    	}
		    	});
		    })();
	    	});
    	}
    });
}

Parser.prototype.finaliseRun = function(db, newOffset)
{
	var self = this;
	var options = this.options;

	this.db.collection(options.state_collection).save({_id: 'last_tweet', value: newOffset}, {w : 1}, function(err, res) {
		if (err) throw err;

		console.log('Done. New offset is ' + newOffset);

		setTimeout(function() {
			self.start(db);
		}, options.check_interval);
	});
}

module.exports = Parser;
