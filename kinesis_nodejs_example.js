// from https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/kinesis-examples-capturing-page-scrolling-full.html

'use strict';

const AWS = require('aws-sdk');
const env = {
    AWS: {
        region: 'us-east-1',
        accessKeyId: "access_key_id",
        secretAccessKey:"secret_key"
    },
    kinesis: {
        apiVersion: '2013-12-02'
    }
};

// AWS Configuration
AWS.config.update(env.AWS);
AWS.config.credentials.get(function(err) {
    // attach event listener
    if (err) {
        alert('Error retrieving credentials.');
        console.error(err);
        return;
    }
    // create Amazon Kinesis service object
    var kinesis = new AWS.Kinesis(env.kinesis);
    let timerId = setInterval(() => {
        var records = [];
        var record = {
            Data : JSON.stringify({
                "code" : 1000,
                "time" : new Date(),
            }),
            PartitionKey: 'partition-value'
        };
        records.push(record);

		kinesis.putRecords({
            Records: records,
            StreamName: 'test_stream'
        }, function(err, data) {
            if (err) {
                console.error(err);
            }else{
                console.log("recorded shardId = ",data.Records[0].ShardId, count++);
            }
        });
    }, 3000);
});
