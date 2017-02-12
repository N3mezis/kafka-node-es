var Q = require('Q'),
    kafka = require('kafka-node');

module.exports = {
    Producer: function (kafkaClient) {
        var producer = new kafka.Producer(kafkaClient),
            initialized = initialize();

        return {
            send: promisedSend,
            on: producer.on,
            createTopics: producer.createTopics
        };

        function promisedSend(topic, messages) {
            var deferred = Q.defer();

            var topicName = typeof topic === 'object' ? topic.name : topic;

            initialized.then(function () {
                try {
                    producer.send([{
                        topic: topicName,
                        messages: messages
                    }], function (err, response) {
                        if (err) {
                            console.error(topicName + '-Producer: ERROR while sending', err);
                            return deferred.reject(err);
                        }

                        if (response) {
                            console.log(topicName + '-Producer: sent', messages);
                            return deferred.resolve(response);
                        }

                        deferred.reject(topicName + '-Procuer: ERROR while sending: neither ERORR, nor ACK received')
                    });
                } catch(err) {
                    deferred.reject(err);
                }

            }, function (err) {
                deferred.reject(err);
            });

            return deferred.promise;
        }

        function initialize() {
            var deferred = Q.defer();

            console.log('Producer: initializing...');

            producer.on('ready', function () {
                console.log('Producer: initialized');
                deferred.resolve();
            });

            producer.on('error', function (err) {
                console.log('Producer: ERROR', err);
                deferred.reject(err);
            });

            return deferred.promise;
        }
    }
};