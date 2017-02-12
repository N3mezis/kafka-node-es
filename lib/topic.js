var uuidGenerator = require('uuid'),
    avro = require('avsc'),
    Q = require('Q'),
    utils = require('./utils'),
    kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.Client('localhost:2181'),
    producer = utils.Producer(client);

module.exports = function Topic(name, schemaDefinition, options) {
    if (!name) {
        throw 'Topic creation failed. Provide a name.';
    }

    if (!schemaDefinition) {
        throw 'Topic creation failed. Provide a schema definition';
    }

    this.name = name;
    this.uuid = uuidGenerator.v1();
    this.schemaDefinition = schemaDefinition;
    this.schema = avro.parse(schemaDefinition);

    this.initialized = registerSchema(this);

    this.publish = publish(this);
    this.subscribe = subscribe(this);
};

function publish(topic) {
    if (!topic) {
        throw 'Cant create publish method without topic';
    }

    if (!topic.schema) {
        return function () {
            throw 'Cannot publish. This topic doesnt have a schema for publishing.'
        }
    }

    return function (data) {
        return topic.initialized.then(function () {
            var parsedData = topic.schema.toBuffer(data);

            return producer.send(topic, [JSON.stringify({
                v: topic.schemaVersion,
                m: Buffer.from(parsedData)
            })]);
        });
    };
}

function subscribe(topic) {
    if (!topic) {
        throw 'Cant create subscribe method without topic';
    }

    if (!topic.schema) {
        return function () {
            throw 'Cannot subscribe. This topic doesnt have a schema for subscribing.'
        };
    }

    topic.subscribers = [];

    topic.initialized.then(function () {
        var consumer = new Consumer(
            new kafka.Client(),
            [{topic: topic.name, partition: topic.partition || 0}]
        );

        consumer.on('message', function (kafkaData) {
            var response = JSON.parse(kafkaData.value);
            console.log(topic.name + '-Consumer: received', response);

            handlePossibleVersionMismatch(topic, response).then(function () {
                delete response.v;
                var message = topic.schema.fromBuffer(new Buffer(response.m));

                topic.subscribers.forEach(function (callback) {
                    callback(message, topic);
                });
            });
        });

        consumer.on('error', function (err) {
            console.log(err);
        });
    }, function (err) {
        console.error('Failed to initialize topic:', err);
    });

    return function (callback) {
        topic.subscribers.push(callback);

        return function unsubscribe() {
            topic.subscribers.splice(subscribers.indexOf(callback), 1);
        };
    };
}

function registerSchema(topic) {
    var deferred = Q.defer();

    console.log('Registering schema');

    console.log('Creating consumer');
    var consumer = new Consumer(
        new kafka.Client('localhost:2181'),
        [{topic: 'schema.register', partition: 0}]
    );
    console.log('Consumer created');

    consumer.on('error', function(err) {
        console.error('Error initializing Consumer for schema registry response', err);
    });

    consumer.on('message', function (kafkaData) {
        console.log('Received schema registry response');

        var response = JSON.parse(kafkaData.value);
        //remove
        response.v = 0;
        response.uuid = topic.uuid;

        if (response.uuid !== topic.uuid) {
            return;
        }

        topic.schemaVersion = response.v;

        consumer.close(function () {
            console.log('Topic is registerd');
            deferred.resolve(response.schema);
        });
    });

    producer.send('schema.register', [
        JSON.stringify({
            uuid: topic.uuid,
            version: 0,
            def: topic.schemaDefinition
        })
    ]);

    return deferred.promise;
}

function handlePossibleVersionMismatch(topic, message) {
    if (message.v === topic.schemaVersion) {
        return Q(true);
    }

    console.log(topic.name + '-Consumer: schema mismatch. ' +
        'Topic version (' + topic.schemaVersion + ') not compatible with message version (' + message.v +')'
    );

    return Q(true);

    return producer.send('schema.mismatch', [
        JSON.stringify({
            uuid: topic.uuid,
            name: topic.name,
            def: topic.schemaDefinition
        })
    ]);
}