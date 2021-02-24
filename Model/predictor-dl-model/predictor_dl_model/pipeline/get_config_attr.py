# Copyright 2019, Futurewei Technologies
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
#                                                 * "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import yaml
import argparse


def config_to_dict(config_file):
    with open(config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
    return cfg


def extract_attribute(config_file, *attrs):
    result = config_to_dict(config_file)
    for attr in attrs:
        result = result[attr]
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Prepare data')
    parser.add_argument('config_file')
    parser.add_argument('root')
    parser.add_argument('child')
    args = parser.parse_args()
    print(extract_attribute(args.config_file, args.root, args.child))
