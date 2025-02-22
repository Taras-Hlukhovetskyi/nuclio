# Kinesis trigger

> **NOTE:**  Kinesis trigger is in tech-preview.

Reads records from [Amazon Kinesis](https://aws.amazon.com/kinesis/) streams.

## In this document
- [Attributes](#attributes)
- [Example](#example)
- [IAM Configuration](#iam-configuration)

## Attributes

| **Path** | **Type** | **Description** |
| :--- | :--- | :--- |
| accessKeyID | string | Required by AWS Kinesis |
| secretAccessKey | string | Required by AWS Kinesis |
| regionName | string | Required by AWS Kinesis |
| streamName | string | Required by AWS Kinesis |
| shards | string | List of shards on which this function receives events |

### Example

```yaml
triggers:
  myKinesisStream:
    kind: kinesis
    attributes:
      accessKeyID: "my-key"
      secretAccessKey: "my-secret"
      regionName: "eu-west-1"
      streamName: "my-stream"
      shards: [shard-0, shard-1, shard-2]
```
> **Note:** If Kinesis is configured with On-Demand capacity mode, the shard ids should be set as:<br>
            `[shardId-000000000000,shardId-000000000001,shardId-000000000002,...]`


### IAM Configuration

The minimal policy-actions needed for Kinesis trigger to consume messages are:

- `kinesis:GetShardIterator`
- `kinesis:GetRecords`
- `kinesis:DescribeStream`

E.g.:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "VisualEditor0",
      "Effect": "Allow",
      "Action": [
        "kinesis:GetShardIterator",
        "kinesis:GetRecords",
        "kinesis:DescribeStream"
      ],
      "Resource": "arn:aws:kinesis:<region-name>:<user-unique-id>:stream/<specific-stream>"
    }
  ]
}
```
