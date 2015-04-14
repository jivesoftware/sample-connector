#!/bin/bash

export RDS_HOSTNAME=localhost
export RDS_PORT=5432
export RDS_DB_NAME=sample-connector
export RDS_USERNAME=postgres
export RDS_PASSWORD=postgres

export __ROLE=all

echo "Starting all roles"
sleep 1
node app.js
