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
var uuid = require('node-uuid');

// The URL to use when connecting to the MQ Light server
var serviceURL = 'amqp://localhost';

// The number of clients that will connect to any given shared destination
var clientsPerSharedDestination = 2;

// The topics to subscribe to for shared destinations
var sharedTopics = ['shared1', 'shared2'];

// The topics to subscribe to for private destinations
var privateTopics = ['private1', 'private2', 'private3', 'private4'];

// All topics.  An entry is picked at random each time a message is sent
var allTopics = sharedTopics.concat(privateTopics);

// A count of all messages sent by the application
var messageCount = 0;

// Text used to compose message bodies.  A random number of words are picked.
var loremIpsum = 'Lorem ipsum dolor sit amet, consectetur adipisicing elit, ' +
                 'sed do eiusmod tempor incididunt ut labore et dolore ' +
                 'magna aliqua. Ut enim ad minim veniam, quis nostrud ' +
                 'exercitation ullamco laboris nisi ut aliquip ex ea ' +
                 'commodo consequat. Duis aute irure dolor in reprehenderit ' +
                 'in voluptate velit esse cillum dolore eu fugiat nulla ' +
                 'pariatur. Excepteur sint occaecat cupidatat non proident, ' +
                 'sunt in culpa qui officia deserunt mollit anim id est ' +
                 'laborum.';

// Build an array of word ending offsets for loremIpsum
var loremIpsumWords = [];
for (var i = 0;;) {
  i = loremIpsum.indexOf(' ', i);
  if (i == -1) {
    loremIpsumWords.push(loremIpsum.length);
    break;
  } else {
    loremIpsumWords.push(i);
    i += 1;
  }
}

// Create clients that subscribe to a shared topic, and send messages
// randomly to any of the topics.
for (var i = sharedTopics.length - 1; i >= 0; i--) {
  for (var j = 0; j < clientsPerSharedDestination; j++) {
    startClient(sharedTopics[i], ('share' + (i + 1)));
  }
}

// Create clients that subscribe to private topics, and send messages
// randomly to any of the topics.
for (var i = privateTopics.length - 1; i >= 0; i--) {
  startClient(privateTopics[i]);
}

// Creates a client.  The client will subscribe to 'topic'.  If the
// 'share' argument is undefined the destination will be private to the
// client.  If the 'share' argument is not undefined, it will be used
// as the share name for subscribing to a shared destination.
// The client is also used to periodically publish a message to a
// randomly chosen topic.
function startClient(topic, share) {
  var opts = {service: serviceURL, id: 'CLIENT_' + uuid.v4().substring(0, 7)};
  var client = mqlight.createClient(opts);

  client.connect(function(err) {
    if (err) {
      console.error('Problem with connect: ', err.message);
      process.exit(1);
    }
  });

  client.on('connected', function() {
    console.log('Connected to ' + client.service + ' using id ' + client.id);
    client.subscribe(topic, share, function(err, topicPattern, share) {
      if (err) {
        console.error('Problem with subscribe request: ', err.message);
        process.exit(1);
      }
      console.log("Receiving messages from topic pattern '" + topicPattern +
                  (share ? "' and share '" + share + "'" : "'"));
    });

    // Loop sending messages to randomly picked topics.  Delay for a small
    // (random) amount of time, each time around.
    var sendMessages = function() {
      var delay = Math.random() * 20000;
      var sendTopic = allTopics[Math.floor(Math.random() * allTopics.length)];
      var sendCallback = function(err, msg) {
        if (err) {
          console.err('Problem with send request: ' + err.message);
          process.exit(0);
        } else {
          if (messageCount == 0) {
            console.log('Sending messages');
          }
          ++messageCount;
          if (messageCount % 10 == 0) {
            console.log('Sent ' + messageCount + ' messages');
          }
        }
      };

      setTimeout(function() {
        var start = Math.floor(Math.random() * (loremIpsumWords.length - 15));
        var end = start + Math.floor(Math.random() * 15);
        var message =
            loremIpsum.substring(loremIpsumWords[start], loremIpsumWords[end]);
        client.send(sendTopic, message, sendCallback);
        sendMessages();
      },delay);
    };
    sendMessages();
  });

}
