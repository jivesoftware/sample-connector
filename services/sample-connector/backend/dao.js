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

function DataAccessObject() {
}

module.exports = DataAccessObject;

/**
 * find all work items owned by the workOwnerID
 * whose modification time is greater than the
 * modification time of the lasted processed item of the workowner
 * @param workOwnerID
 * @returns {promise|Q.promise}
 */
DataAccessObject.prototype.fetchUnprocessedItems = function(workOwnerID) {
    var deferred = q.defer();
    var db = jive.service.persistence();

    db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        // make the query
        dbClient
        .query(
            "select workitems.modtime, payload " +
              "from workitems join workowners on (workitems.workownerid = workowners.workownerid) " +
             "where workitems.workownerid = " + workOwnerID + " " +
               "and workitems.modtime > workowners.modtime " +
             "order by workitems.modtime"
        )

        // process the fetched work items
        .then( function() {
            var r = dbClient.results();
            var workItems = [];
            if (r.rows.length > 0) {
                for ( var i = 0; i < r.rows.length; i++ ) {
                    var workItem = {
                        modtime : parseInt(r.rows[i]['modtime']),
                        payload : r.rows[i]['payload']
                    };
                    workItems.push(workItem);
                }
            }

            return deferred.resolve(workItems);
        })

        // always try to release the client, if it exists
        .finally(function() {
            if ( dbClient ) {
                // always try to release the client, if it exists
                dbClient.release();
            }
        });

    });

    return deferred.promise;
};

/**
 * Attempt to update the lock table, setting the takentime and workerid, for a particular workowner
 * whose takentime is empty or is expired (using the optionally passed in lockExpirationMS)
 * @param workerID
 * @param assignedOwnerID
 * @param lockTime
 * @param lockExpirationMS
 * @returns {promise|Q.promise}
 */
DataAccessObject.prototype.captureLock = function(workerID, assignedOwnerID, lockTime, lockExpirationMS) {
    var deferred = q.defer();
    var db = jive.service.persistence();

    lockExpirationMS = lockExpirationMS || 60 * 1000;

    db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        // make the query
        var now = new Date().getTime();
        dbClient.query(
            "update workowners " +
               "set workerid = " + workerID + ", " +
                   "takentime = " + lockTime + " " +
             "where workownerid = " + assignedOwnerID + " " +
               "and (takentime is NULL or (" + now + " - takenTime > " + lockExpirationMS + ") )"
        )

        // evaluate the result of the attempted lock
        .then(
            // success
            function() {
                var r = dbClient.results();
                deferred.resolve(r.rowCount > 0);
            },

            // failure
            function(e) {
                // log at debug, because update-based lock failures are legit
                jive.logger.debug(e.stack);
                deferred.resolve(false);
            }
        )

        // always try to release the client, if it exists
        .finally(function() {
            if ( dbClient ) {
                // always try to release the client, if it exists
                dbClient.release();
            }
        });
    });

    return deferred.promise;
};

/**
 * Records an activity log for the given workitem (identified by the modtime and workownerid) for the worker
 * which processed it.
 * @param workerid
 * @param workownerid
 * @param modtime
 * @returns {promise|Q.promise}
 */
DataAccessObject.prototype.insertActivity = function(workerid, workownerid, modtime) {
    var deferred = q.defer();
    var db = jive.service.persistence();

    db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        // make the query
        dbClient.query(
            "insert into worklog (workerid, workownerid, modtime) " +
                "values (" + workerid + ", " + workownerid + ", " + modtime + ")"
        )

        // evaluate the results of inserting the activity log
        .then(
            // success
            function() {
                var r = dbClient.results();
                deferred.resolve(r.rowCount > 0);
            },

            // failure
            function(e) {
                jive.logger.debug(e.stack);
                deferred.resolve(false);
            }
        )

        // always try to release the client, if it exists
        .catch(function(e) {
            if ( dbClient ) {
                // always try to release the client, if it exists
                dbClient.release();
            }
        })

        // always try to release the client, if it exists
        .finally(function() {
            if ( dbClient ) {
                // always try to release the client, if it exists
                dbClient.release();
            }
        });
    }).catch( function(e) {
        throw e;
    });

    return deferred.promise;
};

/**
 * Releases the lock table for the given workowner and worker.
 * @param workOwnerID
 * @param workerID
 * @returns {promise|Q.promise}
 */
DataAccessObject.prototype.releaseLock = function(workOwnerID, workerID) {
    var deferred = q.defer();
    var db = jive.service.persistence();

    db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        // make the query
        dbClient.query(
            "update workowners " +
               "set workerid = NULL, " +
                    "takentime = NULL " +
             "where workownerid = " + workOwnerID + " " +
               "and workerid = " + workerID
        )

        // evaluate the result of the attempt to release the lock
        .then(
            // success
            function() {
                var r = dbClient.results();

                if (r.rowCount < 1 ) {
                    jive.logger.warn("workerid " + workerID + " and workOwnerID " + workOwnerID + " was already unlocked.");
                }
                deferred.resolve(true);
            },

            // failure
            function(e) {
                jive.logger.debug(e.stack);
                deferred.resolve(false);
            }
        )

        // always try to release the client, if it exists
        .finally(function() {
            if ( dbClient ) {
                // always try to release the client, if it exists
                dbClient.release();
            }
        });
    });

    return deferred.promise;
};

/**
 * Renews the lock lease to the given worker and its target workowner, and updates
 * the workowner's record of the most recently processed work entry's modtime.
 * @param workOwnerID
 * @param workerID
 * @param modificationTime
 * @returns {promise|Q.promise}
 */
DataAccessObject.prototype.updateLock = function(workOwnerID, workerID, modificationTime) {
    var deferred = q.defer();
    var db = jive.service.persistence();

    db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        var now = new Date().getTime();

        // make the query
        dbClient.query(
            "update workowners " +
               "set modtime = " + modificationTime + ", " +
                   "takentime = " + now + " " +
             "where workownerid = " + workOwnerID + " " +
               "and workerid = " + workerID
        )

        // evaluate the result of the attempt to update modification time
        .then(
            // success
            function() {
                var r = dbClient.results();

                if (r.rowCount < 1 ) {
                    jive.logger.warn("!!!! workOwnerID " + workOwnerID + " failed to have its modtime updated !!!!");
                }
                deferred.resolve(true);
            },

            // failure
            function(e) {
                jive.logger.debug(e.stack);
                deferred.resolve(false);
            }
        )

        // always try to release the client, if it exists
        .finally(function() {
            if ( dbClient ) {
                // always try to release the client, if it exists
                dbClient.release();
            }
        });
    });

    return deferred.promise;
};

/**
 * Adds a new work item for the given workowner. The modificationTime is essentially the unique identifier of the
 * work item.
 * @param workOwnerID
 * @param payload
 * @param modificationTime
 * @returns {promise|Q.promise}
 */
DataAccessObject.prototype.addWork = function(workOwnerID, payload, modificationTime) {
    var deferred = q.defer();
    var db = jive.service.persistence();

    db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        // make the query
        dbClient.query(
            "insert into workitems values (" + workOwnerID + ", '" + payload + "', " + modificationTime + ") "
        )

        // return; no results
        .then(function () {
            deferred.resolve();
        })

        // always try to release the client, if it exists
        .finally(function() {
            if ( dbClient ) {
                // always try to release the client, if it exists
                dbClient.release();
            }
        });
    });

    return deferred.promise;
};

/**
 * Builds up the schema and required seed data to run the system.
 * @returns {*}
 */
DataAccessObject.prototype.setupSchema = function() {
    var db = jive.service.persistence();

    return db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        return dbClient
        // create workitems
        .query(
            'create table if not exists workitems ' +
                '(workownerid bigint, ' +
                'payload text, ' +
                'modtime bigint)'
        )

        // create worklog
        .then( function() {
            return dbClient.query( 'create table if not exists worklog ' +
                '(workownerid bigint, ' +
                'workerid bigint, ' +
                'modtime bigint)');
        })

        // create lock table
        .then( function() {
            return dbClient.query(
                'create table if not exists workowners ' +
                    '(workownerid bigint, ' +
                    'workerid bigint, ' +
                    'takentime bigint, ' +
                    'modtime ' +
                    'bigint)');
        })

        // insert workowners if necessary
        .then( function() {
            return dbClient.query('select count(*) from workowners').then( function() {
                var r = dbClient.results();
                var count = parseInt(r.rows[0]["count"]);
                return q.resolve(count < 1)
            })
            .then( function(insert) {
                if ( insert ) {
                    var insertPromises = [];
                    for ( var i = 1; i <= 5; i++ ) {
                        var insertPromise = dbClient.query("insert into workowners values (" + i + ", NULL, NULL, 0)");
                        insertPromises.push(insertPromise);
                    }
                    return q.all( insertPromises );
                } else {
                    return q.resolve();
                }
            });
        })

        // always try to release the client, if it exists
        .finally(function() {
            if ( dbClient ) {
                // always try to release the client, if it exists
                dbClient.release();
            }
        });
    });

};

function throwError(detail) {
    var error = new Error(detail);
    jive.logger.error(error.stack);
    throw error;
}
