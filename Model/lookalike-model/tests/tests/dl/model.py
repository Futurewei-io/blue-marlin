import tensorflow as tf
import numpy as np

class Seq2Seq(tf.keras.Model):
    def __init__(self,encoder_latent_dim=500,decoder_latent_dim=500,dropout=0.2,testing_ts=20,train_ts=60):
        super(Seq2Seq, self).__init__()
        self.train_ts_index=[i for i in range(train_ts)]
        self.testing_ts_index = [i for i in range(train_ts,train_ts+testing_ts)]
        self.encoder = tf.keras.layers.LSTM(encoder_latent_dim, dropout=dropout, return_state=True,name="encode_lstm")
        self.decoder_lstm = tf.keras.layers.LSTM(decoder_latent_dim, dropout=dropout, return_sequences=True, return_state=True,name="decoder_lstm")
        self.decoder_dense = tf.keras.layers.Dense(1, name="decoder_dense")
    def call(self, inputs, **kwargs):
        encoder_inputs=tf.gather(inputs, indices=self.train_ts_index, axis=1)
        encoder_inputs=tf.reshape(encoder_inputs,shape=(-1,encoder_inputs.shape[1],1))
        encoder_outputs, state_h, state_c = self.encoder(encoder_inputs)
        encoder_states = [state_h, state_c]
        decoder_inputs = tf.gather(inputs, indices=self.testing_ts_index, axis=1)
        decoder_inputs=tf.reshape(decoder_inputs,shape=(-1,decoder_inputs.shape[1],1))
        decoder_outputs, _, _ = self.decoder_lstm(decoder_inputs, initial_state=encoder_states)
        decoder_outputs = self.decoder_dense(decoder_outputs)
        return decoder_outputs
    def summary(self):
        decoder_inputs = tf.keras.layers.Input(shape=(10, 1),name="decoder_inputs")
        encoder_inputs = tf.keras.layers.Input(shape=(10, 1), name="encoder_input")
        tf.keras.Model(inputs=[encoder_inputs,decoder_inputs], outputs=self.call([encoder_inputs,encoder_inputs])).summary()

if __name__ == "__main__":
    model_save_path="G:/tf_log/seq2seq"
    model_learning_rate=0.001
    testing_ts = 40
    train_ts = 60

    time_series = np.random.normal(size=(10,100))

    gpu_devices = tf.config.experimental.list_physical_devices('GPU')
    for device in gpu_devices:
        tf.config.experimental.set_memory_growth(device, True)

    model = Seq2Seq(testing_ts = testing_ts,train_ts=train_ts)

    model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=model_learning_rate),
                  loss=tf.keras.losses.mean_squared_error)

    model.fit(x=time_series[:,:train_ts+testing_ts],y=time_series[:,-testing_ts:], epochs=10)
    model.save(model_save_path)
    model.summary()