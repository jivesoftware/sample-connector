#!/bin/bash

export __ROLE=producer

echo "Starting producer"
sleep 1
if [[ "$PWD" =~ 'scripts' ]]
    then
        cd '../';
fi
node app.js
