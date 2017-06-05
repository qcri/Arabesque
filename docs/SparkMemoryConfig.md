#### Spark-Arabesque Memory Configuration

The following parameters are used to configure the memory when running Arabesque on spark:

| Property Name | Meaning | Default |
| :------------ | :------ | :------ |
| `executor_memory` | This parameter sets the desired memory for each executor.| `1g` |
| `driver_memory` | This parameter sets the desired memory for the driver.| `1g` |
| `max_result_size` | This parameter determines the maximum size of the serialized results that will be returned to the driver from each task (Note: this value should be less than the driver memory or it will cause OOM errors) | `1g` |