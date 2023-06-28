# Delete Records Test

This application can be used to validate the functionality of
[KIP-107 DeleteRecords](https://cwiki.apache.org/confluence/display/KAFKA/KIP-107%3A+Add+deleteRecordsBefore%28%29+API+in+AdminClient)
for the Redpanda LRC test system.

## How it works

This application will start producing records to a Kafka compatible endpoint at a desired throughput.  At intervals
defined by the settings, or through an API call, this test will invoke the DeleteRecords API call at a specific offset.

NOTE: Only one DeleteRecords request will be handled.

The offset that is the deletion point can be set directly (e.g. given a specific offset within a partition) or
relative to the last committed offset and the first committed offset.

Any errors will be reported.

This application will also start sequential consumers and, if requested, consumers that will consume from random offsets
within a topic/partitions.  The random consumer(s) will attempt consumption 

This application will also start consumers that will either function randomly (not yet done) or sequentially and will
be notified of what the start offset the application thinks the partitions currently are at.

## HTTP API

### `/status`

A `GET` to the `/status` endpoint will return:

* LWM and HWM information for each partition
* The last set of records deleted
* Any errors

### `/record`

A `DELETE` to the `/record` endpoint will immediately initiate a delete records request.  This endpoint requires
a single parameter `position` which indicates which position to start record deletion for each topic.  The value values
are:
* `All` - Delete all records
* `Halfway` - delete all records before the halfway point between the high and low watermarks
* `Single` - Delete one record at the low watermark
* `Specific` - Delete specific offsets on specific partitions.  Must provide a JSON schema such as:

```json
{
  "records": [
    {
      "partition": 0,
      "offset": 1
    },
    {
      "partition": 1,
      "offset": 3
    }
  ]
}
```

Example:

`curl -X DELETE localhost:7778/record?position=all`

or

`curl -X DELETE localhost:7778/record?position=specific -d '{"records": [{"partition": 0, "offset": 1},{"partition": 1, "offset": 3}]}'`

### `/service`

A `DELETE` to the `/service` endpoint will shut down the application