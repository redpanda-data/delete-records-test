# Delete Records Test

This application can be used to validate the functionality of
[KIP-107 DeleteRecords](https://cwiki.apache.org/confluence/display/KAFKA/KIP-107%3A+Add+deleteRecordsBefore%28%29+API+in+AdminClient)
for the Redpanda LRC test system.

## How it works

This application will start producing records to a Kafka compatible endpoint at a desired throughput.  At intervals
defined by the settings, or through an API call, this test will invoke the DeleteRecords API call at a specific offset.

NOTE: Only one DeleteRecords request will be handled, all others will be rejected.

The offset that is the deletion point can be set directly (e.g. given a specific offset within a partition) or
relative to the last committed offset and the first committed offset.

Any errors will be reported.

This application will also start consumers that will either function randomly or sequentially and will be notified
of what the start offset the application thinks the partitions currently are at.

## CLI

The following table defines the CLI arguments for the application:

| Argument                           | Description                                         |
|------------------------------------|-----------------------------------------------------|
| `-b/--broker <BROKERS>`            | The brokers to use                                  |
| `-t/--topic <TOPIC_NAME>`          | The name of the _existing_ topic to use             |
| `-p/--port <HTTP PORT>`            | Port to use for API                                 |
| `--compressible-payload`           | Generate compressible payload                       |
| `--compression-type <TYPE>`        | Compression type                                    |
| `--produce-throughput-bps <BPS>`   | Throughput for production                           |
| `--consume-throughput-mbps <MBPS>` | Throughput for consumer (for each consumer)         |
| `--num-consumers <NUM>`            | Number of consumers                                 |
| `--username <USERNAME>`            | Username                                            |
| `--password <PASSWORD>`            | Password                                            |
| `--sasl_mechanism <SASL>`          | Mechanism to use (`SCRAM-SHA256` or `SCRAM-SHA512`) |

## HTTP API