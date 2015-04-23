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

var testUtils = require('jive-testing-framework/testUtils');
var jive = require('jive-sdk');
var apiService = require('./api-services/api-services');
var service = require('./test-service');
var q = require('q');


// Most services expect to be in the root dir - chdir to the root
process.chdir('../');

// Create the test runner - use event handlers to before/after conditions
var makeRunner = function () {
    return testUtils.makeRunner({
        'eventHandlers': {
            'onTestStart': function (test) {
                //We could do some things before each test here
            },
            'onTestEnd': function (test) {
                //We could do some things after each test here
            }
        }
    });
};


/////////////////////////////////////////////////////////////////////////////
// Start our services and run the tests

// check if the test api service is running
apiService.isRunning()

    // if not already running, attempt to start it
    .then(function() {}, function() { return apiService.start() })

    // start the jive services
    .then(function() { return service.start(); })

    // start the test producer
    .then(function() { return service.startTestProducer(); })

    // start the test worker
    .then(function() { return service.startTestWorker(); })

    // start the test runner
    .then(function () {
        return makeRunner().runTests(
            {
                'context': {
                    'testUtils': testUtils,
                    'jive': jive
                },
                'rootSuiteName': 'jive',
                'runMode': 'test',
                'testcases': process.cwd() + '/test/library',
                'timeout': 500000
            })
    })

    // stop the jive services
    .then(function() { return service.stop()})

    // check if the test api service is running
    .then(function () { return apiService.isRunning(); })

    // if it's running, attempt to stop it
    .then(function () { return apiService.stop() })

    // exit
    .then(function () { process.exit(0);});






