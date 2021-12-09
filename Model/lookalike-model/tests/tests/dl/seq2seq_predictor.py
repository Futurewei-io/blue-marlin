import tensorflow as tf
from model import Seq2Seq
import numpy as np

tf.config.run_functions_eagerly(True)
gpu_devices = tf.config.experimental.list_physical_devices('GPU')
for device in gpu_devices:
    tf.config.experimental.set_memory_growth(device, True)


class Encoder(tf.keras.Model):
    def __init__(self,train_model):
        super(Encoder, self).__init__()
        self.encode_lstm = train_model.get_layer("encode_lstm")
    def call(self, inputs, **kwargs):
        encoder_outputs, state_h, state_c = self.encode_lstm(inputs)
        return [state_h, state_c]

class Decoder(tf.keras.Model):
    def __init__(self,train_model):
        super(Decoder, self).__init__()
        self.decoder_lstm = train_model.get_layer("decoder_lstm")
        self.decoder_dense = train_model.get_layer("decoder_dense")
    def call(self, inputs, **kwargs):
        decoder_inputs,decoder_state_input_h, decoder_state_input_c = inputs[0],inputs[1],inputs[2]
        decoder_states_inputs = [decoder_state_input_h, decoder_state_input_c]
        decoder_outputs, state_h, state_c = self.decoder_lstm(decoder_inputs, initial_state=decoder_states_inputs)
        decoder_states = [state_h, state_c]
        decoder_outputs = self.decoder_dense(decoder_outputs)
        output= [decoder_outputs] + decoder_states
        return output

@tf.function
def decode_sequence(encoder_model,decoder_model,input_seq,encode_series_mean):
  batch=input_seq.shape[0]
  states_value = encoder_model.predict(input_seq)
  target_seq = np.zeros((batch, 1, 1))
  target_seq[:batch, 0, 0] = input_seq[:batch, -1, 0]
  decoded_seq = np.zeros((batch, num_days_prediction, 1))
  for i in range(num_days_prediction):
    inp=[target_seq] + states_value
    output, h, c = decoder_model.predict(inp)
    decoded_seq[:batch, i, 0] = output[:batch, 0, 0]
    target_seq = np.zeros((batch, 1, 1))
    target_seq[:batch, 0, 0] = output[:batch, 0, 0]
    states_value = [h, c]
  prediction = np.expm1(decoded_seq + encode_series_mean[:][0])
  prediction=tf.reshape(prediction,shape=(batch,num_days_prediction))
  return prediction

if __name__ == "__main__":
    num_days_prediction=40
    model = Seq2Seq(testing_ts = 40,train_ts=60) # TODO : Replace with Trained model
    encoder_model=Encoder(model)
    decoder_model=Decoder(model)

    time_series = np.random.normal(size=(10,60))
    time_series_mean = np.random.randint(low=1,high=5,size=(10,1))
    time_series = time_series.reshape((time_series.shape[0], time_series.shape[1], 1))
    p=decode_sequence(encoder_model,decoder_model,tf.constant(time_series),tf.constant(time_series_mean))
    # np.savetxt("prediction.np")
    print(p)