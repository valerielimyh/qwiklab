
import argparse
import pandas as pd
import tensorflow as tf
import os #NEW
import json #NEW
from tensorflow.contrib.learn.python.learn import learn_runner
from tensorflow.contrib.learn.python.learn.utils import saved_model_export_utils

print(tf.__version__)
tf.logging.set_verbosity(tf.logging.ERROR)

data_train = pd.read_csv(
  filepath_or_buffer='https://storage.googleapis.com/vijay-public/boston_housing/housing_train.csv',
  names=["CRIM","ZN","INDUS","CHAS","NOX","RM","AGE","DIS","RAD","TAX","PTRATIO","MEDV"])

data_test = pd.read_csv(
  filepath_or_buffer='https://storage.googleapis.com/vijay-public/boston_housing/housing_test.csv',
  names=["CRIM","ZN","INDUS","CHAS","NOX","RM","AGE","DIS","RAD","TAX","PTRATIO","MEDV"])

FEATURES = ["CRIM", "ZN", "INDUS", "NOX", "RM",
            "AGE", "DIS", "TAX", "PTRATIO"]
LABEL = "MEDV"

feature_cols = [tf.feature_column.numeric_column(k)
                  for k in FEATURES] #list of Feature Columns

def generate_estimator(output_dir):
  return tf.estimator.DNNRegressor(feature_columns=feature_cols, 
                                            hidden_units=[args.hidden_units_1, args.hidden_units_2], #NEW (use command line parameters for hidden units)
                                            model_dir=output_dir)

def generate_input_fn(data_set):
    def input_fn():
      features = {k: tf.constant(data_set[k].values) for k in FEATURES}
      labels = tf.constant(data_set[LABEL].values)
      return features, labels
    return input_fn

def serving_input_fn():
  #feature_placeholders are what the caller of the predict() method will have to provide
  feature_placeholders = {
      column.name: tf.placeholder(column.dtype, [None])
      for column in feature_cols
  }
  
  #features are what we actually pass to the estimator
  features = {
    # Inputs are rank 1 so that we can provide scalars to the server
    # but Estimator expects rank 2, so we expand dimension
    key: tf.expand_dims(tensor, -1)
    for key, tensor in feature_placeholders.items()
  }
  return tf.estimator.export.ServingInputReceiver(
    features, feature_placeholders
  )

train_spec = tf.estimator.TrainSpec(
                input_fn=generate_input_fn(data_train),
                max_steps=3000)

exporter = tf.estimator.LatestExporter('Servo', serving_input_fn)

eval_spec=tf.estimator.EvalSpec(
            input_fn=generate_input_fn(data_test),
            steps=1,
            exporters=exporter)

######START CLOUD ML ENGINE BOILERPLATE######
if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  # Input Arguments
  parser.add_argument(
      '--output_dir',
      help='GCS location to write checkpoints and export models',
      required=True
    )
  parser.add_argument(
        '--job-dir',
        help='this model ignores this field, but it is required by gcloud',
        default='junk'
    )
  parser.add_argument(
        '--hidden_units_1', #NEW (expose hyperparameter to command line)
        help='number of neurons in first hidden layer',
        type = int,
        default=10
    )
  parser.add_argument(
        '--hidden_units_2', #NEW (expose hyperparameter to command line)
        help='number of neurons in second hidden layer',
        type = int,
        default=10
    )
  args = parser.parse_args()
  arguments = args.__dict__
  output_dir = arguments.pop('output_dir')
  output_dir = os.path.join(#NEW (give each trial its own output_dir)
      output_dir,
      json.loads(
          os.environ.get('TF_CONFIG', '{}')
      ).get('task', {}).get('trial', '')
  )
######END CLOUD ML ENGINE BOILERPLATE######

  #initiate training job
  tf.estimator.train_and_evaluate(generate_estimator(output_dir), train_spec, eval_spec)