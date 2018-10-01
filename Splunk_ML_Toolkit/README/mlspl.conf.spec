# mlspl.conf contains configuration for the "fit", "apply" and
# "score" commands included with the Machine Learning Toolkit.
#
# Put global settings in the [default] stanza and algorithm-specific
# settings in a stanza named for the algorithm
# (e.g. [LinearRegression] for the LinearRegression algorithm).
#
# Stanzas given by [score:*] do not reference algorithms but rather
# "score" classes (e.g. [score:classification] for classification
# scoring methods such as accuracy_score.


[default]
max_inputs = <int>
* defaults to 100000
* The maximum number of events an algorithm considers when fitting a
  model.
* If this limit is exceeded and use_sampling is true, the fit command
  downsamples its input using the Reservoir Sampling algorithm before
  fitting a model.
* If use_sampling is false and this limit is exceeded, the fit command
  throws an error.

use_sampling = true|false
* defaults to true
* Indicates whether to use Reservoir Sampling for data sets that exceed
  max_inputs or to instead throw an error.

max_fit_time = <int>
* defaults to 600
* The maximum time, in seconds, to spend in the "fit" phase of an
  algorithm.
* This setting does not relate to the other phases of a search (e.g.
  retrieving events from an index).

max_score_time = <int>
* defaults to 600
* The maximum time, in seconds, to spend in the "score" phase of an
  algorithm.
* This setting does not relate to the other phases of a search (e.g.
  retrieving events from an index).

# Maximum time (in seconds) to spend in the "score" phase of a scoring
# method(including down-sampling the input). This does not relate
# to the other phases of a search (e.g. retrieving events from an
# index).
max_score_time = 600

max_memory_usage_mb = <int>
* defaults to 1000
* The maximum allowed memory usage, in megabytes, by the fit command
  while fitting a model.

max_model_size_mb = <int>
* defaults to 15
* maximum allowed size of a model, in megabytes, created by the fit
  command.
* Some algorithms (e.g. SVM and RandomForest) might create unusually
  large models, which can lead to performance problems with bundle
  replication.

handle_new_cat = <string>
* defaults to default
* Action to perform when new value(s) for categorical variable/explanatory
  variable is encountered in partial_fit
  default   : set all values of the column that corresponds to the new
              categorical value to 0's
  skip      : skip over rows that contain the new value(s) and raise a
              warning
  stop      : stop the operation by raising an error

streaming_apply = true|false
* defaults to false
* Setting streaming_apply to true allows the execution of apply command at
  indexer level. Otherwise apply is done on search head.

max_distinct_cat_values = <int>
* defaults to 100
* determines the upper limit for the number of categorical values that will be encoded
in one-hot encoding
* if the number of distinct values exceeds this limit, the field will be dropped
  (with a warning)

max_distinct_cat_values_for_classifiers = <int>
* defaults to 100
* determines the upper limit for the number of distinct values in a categorical field that is 
the target (or response) variable in a classifier algorithm
* if the number of distinct values exceeds this limit, the field will be dropped
  (with a warning)

max_distinct_cat_values_for_scoring = <int>
* defaults to 100
* determines the upper limit for the number of distinct values in a categorical field that is
the target (or response) variable in a scoring method
* if the number of distinct values exceeds this limit, the field will be dropped
  (with an appropriate warning or error message)

# Algorithm-specific configuration
# Note: Not all global settings can be overwritten in algorithm-specific
# section
[Birch]
max_inputs = <int>
* defaults to 2000
* Works well at 20000, but models are quite large.

[KernelPCA]
max_inputs = <int>
* defaults to 5000

[OneClassSVM]
max_inputs = <int>
* defaults to 10000

[SVM]
max_inputs = <int>
* defaults to 10000
* Works well at 20000, but models are quite large.

# This algorithm is especially slow.
[SpectralClustering]
max_fit_time = <int>
* defaults to 1800

max_inputs = <int>
* defaults to 2000

[TFIDF]
max_inputs = <int>
* defaults to 200000

[KernelRidge]
max_inputs = <int>
* defaults to 5000

[DecisionTreeClassifier]
summary_depth_limit = <int>
* defaults to 5

summary_return_json = true|false
* defaults to false

[DecisionTreeRegressor]
summary_depth_limit = <int>
* defaults to 5

summary_return_json = true|false
* defaults to false

[ARIMA]
use_sampling = true|false
* defaults to false

[score:classification]
* defaults to algorithm defaults

[score:pairwise]
max_inputs = <int>
* default to 1000

[score:regression]
* defaults to algorithm defaults

[score:statstest]
* defaults to algorithm defaults

[score:clustering]
* defaults to algorithm defaults


