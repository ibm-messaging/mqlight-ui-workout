/*******************************************************************************
 * Copyright (c) 2014 IBM Corporation and other Contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html 
 *
 * Contributors:
 * IBM - Initial Contribution
 *******************************************************************************/

var mqlight = require('mqlight');
var uuid=require('node-uuid');

// The number of clients that will connect to each share
var numShareSubs = 2;

// The topics that will be shares
var shareTopics = ['my/share', 'another/share'];

// The topics that will not be shares
var nonShareTopics = ['foo', 'bar', 'my/topic', 'another/topic'];

// All the topics in use. Clients will publish to one of these.
var topics = shareTopics.concat(nonShareTopics);

// Set up all the shares...
for (var i = shareTopics.length - 1; i >= 0; i--) {
  for (var j = 0; j < numShareSubs; j++) {
    setup(shareTopics[i], ('share'+(i+1)));
  };
};

// Set up all the non-share subscriptions
for (var i = nonShareTopics.length - 1; i >= 0; i--) {
  setup(nonShareTopics[i]);
};

// Connects a client as specified and starts it sending messages
function setup(subTopic, share) {
  var opts = {  service:'amqp://localhost:5672',id:('DEMO_' + uuid.v4().substring(0, 7))};
  var client = mqlight.createClient(opts);

  client.connect(function(err) {
    if (err) {
      console.log(err);
    }
  });

  client.on('connected', function() {
    console.log('Connected!');
    if (share) {
      var subscription = client.subscribe(subTopic, share, subCallback);
      console.log('Subscribing to topic ' + subTopic + ' and share ' + share);
    } else {
      var subscription = client.subscribe(subTopic, subCallback);
      console.log('Subscribing to topic ' + subTopic);
    }
    sendSomeMessages();
  });

  var subCallback = function(err, address) {
    if (err) {
      console.error('Problem with subscribe request: ' + err.message);
      process.exit(0);
    }
    if (address) {
      console.log("Subscribed to " + address);
    }
  }

  // Sends messages repeatedly to random topics, with small random delays
  function sendSomeMessages() {
    var delay = Math.random()*20000;
    var topic = topics[Math.floor(Math.random()*topics.length)];
    setTimeout(function() {
      var messageLength = Math.ceil(Math.random()*20);
      sendMessage(topic, ('message ' + uuid.v4().substring(0, messageLength)));
      sendSomeMessages();
    },delay);
  }

  function sendMessage(topic, body) {
    client.send(topic, body, function(err, msg) {
      if (err) {
        console.error('Problem with send request: ' + err.message);
        process.exit(0);
      }
    });
  }

}
