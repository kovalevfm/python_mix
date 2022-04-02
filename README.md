python_mix
==========

## mixed python utils

### ZooKeyPartitioner
Partitioner that looks at zookeeper path for changes in resource set, and rebalance if it changes.
It rebalcnce in two cases
* Party changes
* Resources changes

### kafka BatchProducer
Producer that sends batch and wait for answer from kafka (dafult python-kafka producer doesn't wait answer if sends batch)
