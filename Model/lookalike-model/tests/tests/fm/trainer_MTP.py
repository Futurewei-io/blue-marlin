from deep_fm import DeepFM,TF_Record_reader
import tensorflow as tf
import argparse
import pickle


if __name__ == "__main__":
	print("job is started")
	parser = argparse.ArgumentParser()
	parser.add_argument("--data_dir", type=str, help='pickle_file')
	parser.add_argument("--model_save_path", type=str, help='model_save_path')
	parser.add_argument("--num_epochs", type=int, default=1, help='model_save_path')
	parser.add_argument("--model_learning_rate", type=float, default=0.001, help='model_save_path')
	parser.add_argument("--tensorboard_log_dir", type=str, default=0.001, help='model_save_path')

	args, unknown = parser.parse_known_args()

	pickel_file_path = args.data_dir + "/lookalike.pkl"
	tfrecord_location = args.data_dir + "/lookalike_tfrecords/part-r*"
	model_save_path = args.model_save_path
	num_epochs = args.num_epochs
	model_learning_rate = args.model_learning_rate
	tensorboard_log_dir  = args.tensorboard_log_dir

	with open(pickel_file_path, 'rb') as f:
		hyperparam = pickle.load(f)["MODEL"]

	gpu_devices = tf.config.experimental.list_physical_devices('GPU')
	for device in gpu_devices:
		tf.config.experimental.set_memory_growth(device, True)

	model=DeepFM(hyperparam)
	model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=model_learning_rate),loss=tf.keras.losses.binary_crossentropy)

	tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir=tensorboard_log_dir, histogram_freq=1)

	tf_reader=TF_Record_reader(hyperparam,tfrecord_location)
	dataset=tf_reader.get_dataset()

	model.fit(dataset,epochs=num_epochs,callbacks=[tensorboard_callback])
	model.save(model_save_path)
	print("job is completed")