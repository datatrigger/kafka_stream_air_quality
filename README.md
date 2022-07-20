# How to run this project

See also [Get started with Kafka/Python](https://developer.confluent.io/get-started/python/)

### Prerequisites

1) A Kafka cluster. The easiest way to get going is to create a fully managed cluster on [Confluent](https://confluent.cloud)

2) An API token, request one [here](https://aqicn.org/data-platform/token/)

3) A Python 3 environment as described in the *requirements.txt* file. Setting a virtual environment is strongly advised, using `venv` or `virtualenv` for instance.

### Generate the data stream

1) Complete the config_example.ini file with your own cluster's parameters (note: the value of `group.id` is any string of your choice)
2) Create a topic named "air_quality_index" on yhour Kafka cluster
3) Make both *producer.py* and *consumer.py* executable (`chmod u+x <python_script.py>`)
4) Run *producer.py* and *consumer.py* in two separate terminals

Done!
