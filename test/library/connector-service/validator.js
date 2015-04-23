/*
 * Copyright 2013 Jive Software
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

// Simple validation - api returns a response in the form:
//
//      {
//          "success" : true,
//          "reason" : "[possible error text]
//      }
//

exports.validate = function(response) {
    var deferred = q.defer();
    if (response['success']) {
        deferred.resolve();
    }
    else {
        deferred.reject(new Error("Failed: " + response['reason']));
    }
    return deferred.promise;
};