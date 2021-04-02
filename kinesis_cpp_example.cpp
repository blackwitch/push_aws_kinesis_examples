// from https://docs.aws.amazon.com/code-samples/latest/catalog/cpp-kinesis-put_get_records.cpp.html

/*
Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

This file is licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License. A copy of
the License is located at

http://aws.amazon.com/apache2.0/

This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
*/

#include <iostream>
#include <ctime>
#include <iomanip>
#include <random>
#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/kinesis/KinesisClient.h>
#include <aws/kinesis/model/DescribeStreamRequest.h>
#include <aws/kinesis/model/DescribeStreamResult.h>
#include <aws/kinesis/model/GetRecordsRequest.h>
#include <aws/kinesis/model/GetRecordsResult.h>
#include <aws/kinesis/model/GetShardIteratorRequest.h>
#include <aws/kinesis/model/GetShardIteratorResult.h>
#include <aws/kinesis/model/Shard.h>
#include <aws/kinesis/model/PutRecordsResult.h>
#include <aws/kinesis/model/PutRecordsRequest.h>
#include <aws/kinesis/model/PutRecordsRequestEntry.h>

int main()
{
	Aws::SDKOptions options;
	Aws::InitAPI(options);
	// set user access key id and secret key here
	Aws::Auth::AWSCredentials credentials(Aws::String("access_key_id"), Aws::String("secret_key"));

	{
		//set kinesis data steams name
		const Aws::String streamName("test_stream"); 

		std::random_device rd;
		std::mt19937 mt_rand(rd());

		Aws::Client::ClientConfiguration clientConfig;
		// set your region
		clientConfig.region = Aws::Region::US_EAST_1;
		Aws::Kinesis::KinesisClient kinesisClient(credentials, clientConfig);

		Aws::Kinesis::Model::PutRecordsRequest putRecordsRequest;
		putRecordsRequest.SetStreamName(streamName);
		Aws::Vector<Aws::Kinesis::Model::PutRecordsRequestEntry> putRecordsRequestEntryList;

		// send json format data to kinesis
		{
			Aws::Kinesis::Model::PutRecordsRequestEntry putRecordsRequestEntry;
			Aws::StringStream pk;
			pk << "pk-" << (i % 100);
			putRecordsRequestEntry.SetPartitionKey(pk.str());
			Aws::StringStream data;

			auto t = std::time(nullptr);
			struct tm timeinfo;
			localtime_s(&timeinfo, &t);

			data << "{\"Code\":1000, \"time\": \"" << std::put_time(&timeinfo, "%Y-%m-%d %H:%M:%S") << "\"}";
			std::cout << data.str();
			Aws::Utils::ByteBuffer bytes((unsigned char*)data.str().c_str(), data.str().length());
			putRecordsRequestEntry.SetData(bytes);
			putRecordsRequestEntryList.emplace_back(putRecordsRequestEntry);
		}
		putRecordsRequest.SetRecords(putRecordsRequestEntryList);
		Aws::Kinesis::Model::PutRecordsOutcome putRecordsResult = kinesisClient.PutRecords(putRecordsRequest);

		// if one or more records were not put, retry them
		while (putRecordsResult.GetResult().GetFailedRecordCount() > 0)
		{
			std::cout << "Some records failed, retrying" << std::endl;
			Aws::Vector<Aws::Kinesis::Model::PutRecordsRequestEntry> failedRecordsList;
			Aws::Vector<Aws::Kinesis::Model::PutRecordsResultEntry> putRecordsResultEntryList = putRecordsResult.GetResult().GetRecords();
			for (unsigned int i = 0; i < putRecordsResultEntryList.size(); i++)
			{
				Aws::Kinesis::Model::PutRecordsRequestEntry putRecordRequestEntry = putRecordsRequestEntryList[i];
				Aws::Kinesis::Model::PutRecordsResultEntry putRecordsResultEntry = putRecordsResultEntryList[i];
				if (putRecordsResultEntry.GetErrorCode().length() > 0)
					failedRecordsList.emplace_back(putRecordRequestEntry);
			}
			putRecordsRequestEntryList = failedRecordsList;
			putRecordsRequest.SetRecords(putRecordsRequestEntryList);
			putRecordsResult = kinesisClient.PutRecords(putRecordsRequest);
		}
	}
	Aws::ShutdownAPI(options);

	return 0;
}
