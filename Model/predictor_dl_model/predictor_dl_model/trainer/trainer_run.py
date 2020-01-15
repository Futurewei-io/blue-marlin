import os

if __name__ == '__main__':
    os.system('python tfrecord_reader.py --add_days=63 data/vars')
    os.system('python trainer.py --name s32 --hparam_set=s32 --n_models=3 --no_forward_split --no_eval --asgd_decay=0.99 --max_epoch=150')
