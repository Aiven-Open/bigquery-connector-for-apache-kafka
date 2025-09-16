#!/bin/bash
#
# Copyright 2024 Copyright 2022 Aiven Oy and
# bigquery-connector-for-apache-kafka project contributors
#
# This software contains code derived from the Confluent BigQuery
# Kafka Connector, Copyright Confluent, Inc, which in turn
# contains code derived from the WePay BigQuery Kafka Connector,
# Copyright WePay, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

git fetch origin
if [ -z $1 ]
then
  echo "Must provide final version"
  exit 1
fi

startTag=`grep latestRelease pom.xml | cut -f2 -d">" | cut -f1 -d"<"`
endTag=${1}

start=`git rev-parse v${startTag}`
end=`git rev-parse HEAD`
commits=${start}...${end}
printf "## v%s\n### What's changed\n" ${endTag}
git log --format=' - %s'  ${commits}

printf "\n\n### Co-authored by\n"
git log --format=' - %an'  ${commits} | sort -u
printf "\n\n### Full Changelog\nhttps://github.com/Aiven-Open/bigquery-connector-for-apache-kafka/compare/v${startTag}...v${endTag}\n\n"
