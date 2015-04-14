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
var BaseDAO = require('./BaseDAO');
var util = require("util");

var DataAccessObject = function() {
};

util.inherits(DataAccessObject, BaseDAO);

module.exports = DataAccessObject;

/**
 * find all work items owned by the workOwnerID
 * whose modification time is greater than the
 * modification time of the lasted processed item of the workowner
 * @param workOwnerID
 * @returns {promise|Q.promise}
 */
DataAccessObject.prototype.fetchUnprocessedItems = function(workOwnerID) {
    return this.query( function(dbClient) {

        // make the query
        return dbClient
            .query(
                "select workitems.modtime, payload " +
                    "from workitems join workowners on (workitems.workownerid = workowners.workownerid) " +
                    "where workitems.workownerid = $1 " +
                    "and workitems.modtime > workowners.modtime " +
                    "order by workitems.modtime",

                [workOwnerID]
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
                return q.resolve(workItems);
            });
    });
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
    lockExpirationMS = lockExpirationMS || 60 * 1000;

    return this.query( function(dbClient) {

        // make the query
        var now = new Date().getTime();
        return dbClient.query(
                "update workowners " +
                    "set workerid = $1, " +
                    "takentime = $2 " +
                    "where workownerid = $3 " +
                    "and (takentime is NULL or ($4 - takenTime > $5 ) )",
                [workerID, lockTime, assignedOwnerID, now, lockExpirationMS ]
            )

            // evaluate the result of the attempted lock
            .then(
                // success
                function() {
                    var r = dbClient.results();
                    return q.resolve(r.rowCount > 0);
                },

                // failure
                function(e) {
                    // log at debug, because update-based lock failures are legit
                    jive.logger.debug(e.stack);
                    return q.resolve(false);
                }
            )
    });
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
    return this.query( function(dbClient) {

        // make the query
       return dbClient.query(
                "insert into worklog (workerid, workownerid, modtime) " +
                    "values ($1, $2, $3)",

                [workerid, workownerid, modtime]
            )

            // evaluate the results of inserting the activity log
            .then(
                // success
                function() {
                    var r = dbClient.results();
                    return q.resolve(r.rowCount > 0);
                },

                // failure
                function(e) {
                    jive.logger.debug(e.stack);
                    return q.resolve(false);
                }
            )
    });
};

/**
 * Releases the lock table for the given workowner and worker.
 * @param workOwnerID
 * @param workerID
 * @returns {promise|Q.promise}
 */
DataAccessObject.prototype.releaseLock = function(workOwnerID, workerID) {
    return this.query( function(dbClient) {

        // make the query
        return dbClient.query(
                "update workowners " +
                    "set workerid = NULL, " +
                    "takentime = NULL " +
                    "where workownerid = $1 " +
                    "and workerid = $2",

                [workOwnerID, workerID]
            )

            // evaluate the result of the attempt to release the lock
            .then(
                // success
                function() {
                    var r = dbClient.results();

                    if (r.rowCount < 1 ) {
                        jive.logger.warn("workerid " + workerID + " and workOwnerID " + workOwnerID + " was already unlocked.");
                    }
                    return q.resolve(true);
                },

                // failure
                function(e) {
                    jive.logger.debug(e.stack);
                    return q.resolve(false);
                }
            )
    });
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
    return this.query( function(dbClient) {
       var now = new Date().getTime();

       // make the query
       return dbClient.query(
                "update workowners " +
                    "set modtime = $1, " +
                    "takentime = $2 " +
                    "where workownerid = $3 " +
                    "and workerid = $4",

                [modificationTime, now, workOwnerID, workerID]
            )

            // evaluate the result of the attempt to update modification time
            .then(
            // success
            function() {
                var r = dbClient.results();

                if (r.rowCount < 1 ) {
                    jive.logger.warn("!!!! workOwnerID " + workOwnerID + " failed to have its modtime updated !!!!");
                }
                return q.resolve(true);
            },

            // failure
            function(e) {
                jive.logger.debug(e.stack);
                return q.resolve(false);
            }
        )
    });
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
    return this.query( function(dbClient) {
        // make the query
        return dbClient.query(
            "insert into workitems values ($1, $2, $3)",
            [workOwnerID, payload, modificationTime]
        )

        // return; no results
        .then(function () {
            return q.resolve();
        })
    });
};

/**
 * Builds up the schema and required seed data to run the system.
 * @returns {*}
 */
DataAccessObject.prototype.setupSchema = function() {
    var db = jive.service.persistence();
    var self = this;

    return db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            self.throwError("Can't query, invalid client");
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
                var count = 0;
                var resultRow = r.rows[0];
                for ( var k in  resultRow ) {
                    if (resultRow.hasOwnProperty(k) && k.indexOf('count') > -1 ) {
                        count =  parseInt(resultRow[k]);
                    }
                }
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
    })
    .fail( function(e) {
        self.throwError(e);
    });
};