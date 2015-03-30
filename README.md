# Sample Connector

## Concepts

**Owners** are like Jive instances. They identify related work items that should be processed sequentially, never concurrently.

**Workers** attempt to acquire an owner based on interval and recovery rules. If acquisition fails it is assumed that another worker has acquired the owner.

**Log** entries contain work that has been processed.

## Setup

This example requires node.js to be installed. After cloning the git repository, run `npm install` on the command line in pull down all required dependencies.

The default configuration of this example requires Postgres to be installed on localhost with a user "postgres" with the password "postgres". Otherwise you will need to update the database URL in jiveclientconfiguration.json to point to the proper database instance with the proper credentials. Ensure the postgres user has permission to manipulate database objects.

Create a database named "sample-connector" in your postgres server. All necessary tables will be created the first time the demo is run.

To run the watch.sh shell script, `psql` needs to be in you PATH, and you need to be able to connect to the database at localhost as the "postgres" user without a password. If your current configuration does not support this the watch.sh script will require much editing.

## Running the Demo

Three scripts are provided:

1. **scripts/watch.sh**: Runs a few SQL queries every second to watch the queue.
2. **scripts/producer.sh** Starts the producer process. This process adds items into the work item queue.
3. **scripts/worker.sh** Starts a worker process. This process consumes and runs work items from the work items queue.

Run these scripts from the project root directory using the commands in bold above.

### scripts/watch.sh
The watch.sh script polls the database, displaying significant data about the status of the queue and the worker(s) consuming from it.

### scripts/producer.sh
The producer.sh script adds work items to the database. Items are added about three times faster than a single worker can process them.

_**Do not run more than one instance of producer.sh at a time.**_

### scripts/worker.sh
The worker.sh script requires a single numeric parameter between 1 and 5.

Up to five workers can be run at the same time, so long as they each have a different worker ID.
