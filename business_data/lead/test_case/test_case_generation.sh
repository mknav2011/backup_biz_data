#!/usr/bin/env bash

aws dynamodb put-item --table-name move-dataeng-last-good-key --item file://./pdt_last_good_key_dynamodb_20180110.json

aws dynamodb put-item --table-name move-dataeng-last-good-key --item file://./pdt_last_good_key_dynamodb_20180111.json
