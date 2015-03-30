var jive = require('jive-sdk');
var q = require('q');
var DataAccessObject = require('./dao');
var Producer = require('./producer');
var Consumer = require('./consumer');

exports.onBootstrap = function(app) {
    jive.logger.info("****************************************");
    jive.logger.info("Sample distributed workqueue processing");
    jive.logger.info("****************************************");

    var error;

    // setup the schema
    (function() {
        var dao = new DataAccessObject();
        return dao.setupSchema();
    })()

    // launch the workers
    .then( function() {
        var role = process.env.__ROLE;

        if ( role === 'producer' ) {
            var producer = new Producer();
            producer.launch();
        }

        if ( role === 'consumer' ) {
            var consumer = new Consumer();
            consumer.launch();
        }
    })
    .catch(function(e) {
        error = e;
    })
    .done(function() {
        if ( error ) {
            throw error;
        }
    });
};


