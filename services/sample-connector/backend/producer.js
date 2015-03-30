var jive = require('jive-sdk');
var q = require('q');
var DataAccessObject = require('./dao');

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Public API

function Producer() {
    jive.logger.info("******************************");
    jive.logger.info("* producer *");
    jive.logger.info("******************************");

    this.dao = new DataAccessObject();
}

module.exports = Producer;

Producer.prototype.launch = function() {
    var self = this;

    var produceWorkItem = function () {
        var ownerID = getRandomInt(1, 5);
        var payload = jive.util.guid();
        var modificationTime = new Date().getTime();

        self.dao.addWork(ownerID, payload, modificationTime).then(function() {
            jive.logger.info("Inserted ownerID " + ownerID + ", payload " + payload);
        });
    };

    // schedule
    var producer_rate = process.env.__PRODUCER_RATE || 250;
    var id = jive.util.guid();
    var task = jive.tasks.build(produceWorkItem, producer_rate, id);
    jive.tasks.schedule( task, jive.service.scheduler());
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Private

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

