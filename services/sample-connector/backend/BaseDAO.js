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
 * Template function for correct sql execution.
 * 1. acquire a client - throw an error if non can be found
 * 2. run the operation (the parameter to this function)
 * 3. release the client
 * @param operation
 * @returns {*}
 */
DataAccessObject.prototype.query = function(operation) {
    var db = jive.service.persistence();
    var pClient = db.getQueryClient();

    return pClient.then( function(dbClient) {
        if ( !dbClient ) {
            throwError("Can't query, invalid client");
        }

        var result = operation(dbClient);
        if ( !result ) {
            throwError("Operation must return a promise.");
        }

        // always try to release the client, if it exists
        result.finally(function() {
            if ( dbClient ) {
                // always try to release the client, if it exists
                dbClient.release();
            }
        });

        return result;
    })
    // failed to get a db connection
    .fail( function(e) {
        jive.logger.error(e.stack);
        return q.reject(e);
    });
};

function throwError(detail) {
    var error = new Error(detail);
    jive.logger.error(error.stack);
    throw error;
}
