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

DataAccessObject.prototype.fetchUnprocessedItems = function(assignedOwnerID) {
    var deferred = q.defer();
    var db = jive.service.persistence();

    db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        // make the query
        dbClient
        .query(
            "select workqueue.modtime, payload " +
            "from workqueue join owners on (workqueue.ownerid = owners.ownerid) " +
            "where owners.ownerid = " + assignedOwnerID + " " +
                   "and workqueue.modtime > owners.modtime"
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

            // when done processing return the maximum modification time
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

DataAccessObject.prototype.captureLock = function(consumerID, assignedOwnerID, lockTime) {
    var deferred = q.defer();
    var db = jive.service.persistence();

    db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        // make the query
        var now = new Date().getTime();
        dbClient.query(
            "update owners " +
                "set workerid = " + consumerID + ", " +
                "takentime = " + lockTime + " " +
                "where " +
                "ownerid = " + assignedOwnerID + " " +
                "and (takentime is NULL or (" + now + " - takenTime > 10000) )"
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

DataAccessObject.prototype.insertActivity = function(workerid, ownerid, modtime) {
    var deferred = q.defer();
    var db = jive.service.persistence();

    db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        // make the query
        dbClient.query(
            "insert into activitylog (workerid, ownerid, modtime) " +
                "values (" + workerid + ", " + ownerid + ", " + modtime + ")"
        )

        // evaluate the results of inserting the activity log
        .then(
            // success
            function() {
                var r = dbClient.results();
                if ( !r ) {
                    console.log();
                }
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

DataAccessObject.prototype.releaseLock = function(assignedOwnerID, consumerID, maxModificationTime) {
    var deferred = q.defer();
    var db = jive.service.persistence();

    db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        // make the query
        dbClient.query(
            "update owners " +
                "set workerid = NULL, " +
                "modtime = " + (maxModificationTime ? maxModificationTime : "modtime") + ", " +
                "takentime = NULL " +
                "where " +
                "ownerid = " + assignedOwnerID + " " +
                "and workerid = " + consumerID
        )

        // evaluate the result of the attempt to release the lock
        .then(
            // success
            function() {
                var r = dbClient.results();

                if (r.rowCount < 1 ) {
                    jive.logger.warn("consumer " + consumerID + " and ownerID " + assignedOwnerID + " was already unlocked.");
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

DataAccessObject.prototype.addWork = function(ownerID, payload, modificationTime) {
    var deferred = q.defer();
    var db = jive.service.persistence();

    db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        // make the query
        dbClient.query(
            "insert into workqueue values (" + ownerID + ", '" + payload + "', " + modificationTime + ") "
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

DataAccessObject.prototype.setupSchema = function() {
    var db = jive.service.persistence();

    return db.getQueryClient().then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        return dbClient
        // create workqueue
        .query(
            'create table if not exists workqueue ' +
                '(ownerid bigint, ' +
                'payload text, ' +
                'modtime bigint)'
        )

        // create activitylog
        .then( function() {
            return dbClient.query( 'create table if not exists activitylog ' +
                '(ownerid bigint, ' +
                'workerid bigint, ' +
                'modtime bigint)');
        })

        // create lock table
        .then( function() {
            return dbClient.query(
                'create table if not exists owners ' +
                    '(ownerid bigint, ' +
                    'workerid bigint, ' +
                    'takentime bigint, ' +
                    'modtime ' +
                    'bigint)');
        })

        // insert owners if necessary
        .then( function() {
            return dbClient.query('select count(*) from owners').then( function() {
                var r = dbClient.results();
                var count = parseInt(r.rows[0]["count"]);
                return q.resolve(count < 1)
            })
            .then( function(insert) {
                if ( insert ) {
                    var insertPromises = [];
                    for ( var i = 1; i <= 5; i++ ) {
                        var insertPromise = dbClient.query("insert into owners values (" + i + ", NULL, NULL, 0)");
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