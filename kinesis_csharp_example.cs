// from https://github.com/aws-samples/aws-net-guides/blob/master/Storage/Kinesis/SampleApplication/Program.cs

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Newtonsoft.Json;

namespace KinesisPublishSample
{
    public class LogSampleData
    {
        public string time { get; set; }
        public int code { get; set; }
    }

    class Program
    {
        // NOTE: replace the value with your Kinesis stream name
        static string _kinesisStreamName = "test_stream";

        // NOTE: update with the region in which you created your stream
        static Amazon.RegionEndpoint _regionEndpoint = Amazon.RegionEndpoint.USEast1;

        static int _maxExecutionCount = 1000;
        static int _publishInterval = 3000;

        static bool _cancelled = false;

        static void Main(string[] args)
        {
            Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);

            //  it will push log data maximum 2x1000 count
            for(int ec=0; ec< _maxExecutionCount; ec++)
            {
                //  add log data in a list or queue
                List<LogSampleData> dataList = GetLogDataList(2);
                //  push log data 
                PublishDeviceDataToKinesis(dataList);
                //  wait for 3 sec
                Thread.Sleep(_publishInterval);

                //  will you stop?
                if (_cancelled) break;
            }

            Console.WriteLine("Task Completed!\n");
            Console.Write("To publish more data, please run the application again.\n");

            Console.CancelKeyPress -= new ConsoleCancelEventHandler(Console_CancelKeyPress);
        }

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            if (e.SpecialKey == ConsoleSpecialKey.ControlC)
            {
                _cancelled = true;
                e.Cancel = true;
            }
        }

        private static void PublishDeviceDataToKinesis(List<LogSampleData> dataList)
        {
            var aws_cred = new Amazon.Runtime.BasicAWSCredentials("access_key_id", "secret_key");
            var kinesisClient = new AmazonKinesisClient(aws_cred, _regionEndpoint);

            foreach (LogSampleData data in dataList)
            {
                var dataAsJson = JsonConvert.SerializeObject(data);
                var dataAsBytes = Encoding.UTF8.GetBytes(dataAsJson);
                using (var memoryStream = new MemoryStream(dataAsBytes))
                {
                    try
                    {
                        var requestRecord = new PutRecordRequest
                        {
                            StreamName = _kinesisStreamName,
                            PartitionKey = data.time,
                            Data = memoryStream
                        };

                        var responseRecord = kinesisClient.PutRecordAsync(requestRecord).Result;
                        Console.WriteLine($"Successfully published. Record:{data.log_code},{data.title_code},{data.time} Seq:{responseRecord.SequenceNumber}");

                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed to publish. Exception: {ex.Message}");
                    }
                }
            }
        }

        private static List<LogSampleData> GetLogDataList(int _sampleLogCount)
        {
            var dataList = new List<LogSampleData>();

            var url = Path.GetRandomFileName();
            for (var i = 0; i < _sampleLogCount; i++)
            {
                var rnd = new Random(Guid.NewGuid().GetHashCode());

                var data = new LogSampleData
                {
                    time = System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
                    code = 1000,
                };
                dataList.Add(data);
            }
            return dataList;
        }
    }
}
