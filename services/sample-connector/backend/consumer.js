/*
 * Copyright 2015 Jive Software
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

var jive = require('jive-sdk');
var q = require('q');
var DataAccessObject = require('./dao');

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Public API

/**
 * This entity consumes items from the workqueue table.
 * @constructor
 */
function Consumer(consumerID) {
    if ( !consumerID ) {
        consumerID = process.env.__CONSUMER_ID;
        if ( !consumerID ) {
            consumerID = new Date().getTime();
        }
    }

    jive.logger.info("******************************");
    jive.logger.info("* consumer ", consumerID);
    jive.logger.info("******************************");

    this.consumerID = consumerID;
    this.dao = new DataAccessObject();
}

module.exports = Consumer;

Consumer.prototype.launch = function() {
    var self = this;

    // schedule
    var producer_lock_rate = process.env._CONSUMER_LOCK_RATE || 1000;
    var id = jive.util.guid();

    var task = jive.tasks.build(
        function() {
            return captureLock.call( self );
        },
        producer_lock_rate, id);
    jive.tasks.schedule( task, jive.service.scheduler());
};

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Private

function performWorkItem(consumerID, ownerID, workItem) {
    var processTimeInflation = 150;
    var deferred = q.defer();
    var self = this;

    setTimeout( function() {
        // log the activity, so we can make sure that no single work entry
        // was handled by the same consumer (eg. processed twice)
        self.dao.insertActivity(consumerID, ownerID, workItem['modtime']).then( function() {
            jive.logger.info(">> consumer " + consumerID
                + " processed payload id/modtime " + workItem['modtime']
                + " with payload " + workItem['payload']
                + " for ownerID " + ownerID
            );
        }).catch( function(e) {
            jive.logger.error(e.stack);
        }).finally( function(e) {
            deferred.resolve();
        });
    }, processTimeInflation + getRandomInt(0, 500) );

    return deferred.promise;
}

/**
 * Fetches work items whose modification date is after the most recently processed
 * work item modification date that is stored in the lock table row for the specified user.
 * @param ownerID
 * @returns {promise|Q.promise}
 */
function processLock(ownerID) {
    var self = this;
    var deferred = q.defer();

    self.dao
        // fetch processable work items
        .fetchUnprocessedItems(ownerID )

        // process the fetched work items
        .then( function(items) {
            var maxModificationTime;
            var promises = [];

            for ( var i = 0; i < items.length; i++ ) {
                var promise = function() {
                    var workItem = items[i];
                    if ( !maxModificationTime ) {
                        maxModificationTime = 0;
                    }
                    maxModificationTime = workItem['modtime'] > maxModificationTime ?
                        workItem['modtime'] : maxModificationTime ;

                    return performWorkItem.call(self, self.consumerID, ownerID, workItem);
                };
                promises.push(promise());
            }

            q.all( promises).then(
                function(){
                    deferred.resolve(maxModificationTime);
                },
                function(e) {
                    jive.logger.error(e.stack);
                    deferred.reject(e);
                }
            ).catch( function(e) {
                jive.logger.error(e.stack);
                deferred.reject(e);
            });
        });

    return deferred.promise;
}

/**
 * Captures a lock on the work queue items for one of the owners.
 */
function captureLock() {
    var self = this;

    var assignedOwnerID = getRandomInt(1, 5);
    if ( !assignedOwnerID ) {
        throw new Error("Not assigned an owner!");
    }

    var now = new Date().getTime();
    return self.dao
        .captureLock(self.consumerID, assignedOwnerID, now )
        .then(
        // success
        function(captured) {
            if (captured) {
                jive.logger.info("consumer " + self.consumerID + " locked ownerID " + assignedOwnerID);
                processLock.call(self, assignedOwnerID).then( function(maxModificationTime) {
                    releaseLock.call(self, assignedOwnerID, maxModificationTime);
                });
            } else {
                jive.logger.info("consumer " + self.consumerID +
                    " failed to lock ownerID " + assignedOwnerID);
            }
        },

        // failure
        function(e) {
            jive.logger.error("exception caused consumer " + self.consumerID +
                " failed to lock ownerID " + assignedOwnerID, e.stack);
        }
    );
}

/**
 * Releases the workqueue lock for the specified owner, and sets the last modification time as specified.
 * @param assignedOwnerID
 * @param maxModificationTime
 * @returns {promise|Q.promise}
 */
function releaseLock(assignedOwnerID, maxModificationTime) {
    var self = this;
    var deferred = q.defer();

    self.dao
        .releaseLock(assignedOwnerID, self.consumerID, maxModificationTime)
        .then(
        // success
        function(released) {
            if (released) {
                jive.logger.info("consumer " + self.consumerID +
                    " unlocked ownerID " + assignedOwnerID);
            } else {
                jive.logger.error("consumer " + self.consumerID +
                    " failed to unlock ownerID " + assignedOwnerID);
            }
            deferred.resolve();
        },

        // failure
        function(e) {
            jive.logger.error("exception consumer " + self.consumerID +
                " failed to unlock ownerID " + assignedOwnerID, e.stack);
            deferred.reject(e);
        }
    );

    return deferred.promise;
}

function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}




