# Copyright 2020, Futurewei Technologies
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


from os.path import join as path_join
from airflow import DAG
import datetime as dt
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import timedelta

default_args = {
    'owner': 'din_model_trainer',
    'depends_on_past': False,
    'start_date': dt.datetime(2020, 7, 22),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'din_model_trainer',
    default_args=default_args,
    schedule_interval=None
)


class CommandsBuilder(object):
    class CommandBuilder(object):
        def __init__(self, command):
            super(CommandsBuilder.CommandBuilder, self).__init__()
            self._command = command
            self._params = []

        def add_param(self, param):
            self._params.append(param)
            return self

        def build(self):
            return '{} {}'.format(self._command, ' '.join(self._params))

    def __init__(self, commands):
        super(CommandsBuilder, self).__init__()
        self._commands = commands

    def build(self):
        commands = ' ; '.join(self._commands)
        return commands


def sshOperator(
        task_id,
        command,
        **kwargs
):
    return SSHOperator(
        get_pty=True,
        ssh_conn_id='ssh_105',
        command=command,
        dag=dag,
        task_id=task_id,
        **kwargs
    )


# 'python /home/faezeh/Desktop/bluemarlin-models/din_model/din_model/trainer/{}'.format(file)

HOME = '/home/faezeh'
DIN_MODEL_PATH = path_join(HOME, 'Desktop/bluemarlin-models/din_model/din_model')
TRAINER_PATH = path_join(DIN_MODEL_PATH, 'trainer')
CONDA_HOME = path_join(HOME, 'anaconda3')
CONDA_BIN = path_join(CONDA_HOME, 'bin')
CONDA_PATH = path_join(CONDA_BIN, 'conda')

init_commands = [
    'PATH=$PATH:' + CONDA_BIN,
    '. ' + path_join(CONDA_HOME, 'etc/profile.d/conda.sh'),
    'conda activate py3.6',
    'cd ' + DIN_MODEL_PATH,
    'python --version'
]

build_commands = [
    CommandsBuilder.CommandBuilder('python')
    .add_param('trainer/gdin_build_dataset.py')
    .add_param('config.yml')
    .build()
]

train_commands = [
    CommandsBuilder.CommandBuilder('python')
    .add_param('trainer/train.py')
    .add_param('config.yml')
    .build()
]

save_model_commands = [
    CommandsBuilder.CommandBuilder('python')
    .add_param('trainer/save_model.py')
    .add_param('--model_version=10')
    .add_param('--data_dir=trainer')
    .add_param('--ckpt_dir=trainer/save_path')
    .add_param('--saved_dir=trainer')
    .add_param('--predict_ads_num=30')
    .build()
]

rest_client_commands = [
    CommandsBuilder.CommandBuilder('python')
    .add_param('trainer/rest_client.py')
    .add_param('config.yml')
    .build()
]

tensorflow_commands = [
    CommandsBuilder.CommandBuilder('TENSORFLOW_MODEL_NAME=whatever')
    .build(),
    CommandsBuilder.CommandBuilder('docker')
    .add_param('run')
    .add_param('-p')
    .add_param('8501:8501')
    .add_param('--mount')
    .add_param('type=bind,source=/tmp/tfserving,target=/models/$TENSORFLOW_MODEL_NAME')
    .add_param('-e')
    .add_param('MODEL_NAME=$TENSORFLOW_MODEL_NAME')
    .add_param('-t')
    .add_param('tensorflow/serving')
    .build()
]

build = sshOperator('build_dataset', CommandsBuilder(init_commands + build_commands).build())
train = sshOperator('train', CommandsBuilder(init_commands + train_commands).build())
save_model = sshOperator('save_model',
                         CommandsBuilder(init_commands + save_model_commands).build())

# build
build >> train >> save_model  # >> tensorflow >> rest_client
