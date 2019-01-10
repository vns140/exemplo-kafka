using System;
using Confluent.Kafka;

namespace client.kafka
{
    public class Producer
    {
        public async void PublishAsync()
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };

            // A Producer for sending messages with null keys and UTF-8 encoded values.
            using (var p = new Producer<Null, string>(config))
            {
                try
                {
                    for (int i = 0; i < 12000; ++i)
                    {
                       var dr = await p.ProduceAsync("topico-teste", new Message<Null, string> { Value = i.ToString() });
                       Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                    }

                    
                }
                catch (KafkaException e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }

        public void Publish()
        {
            var conf = new ProducerConfig { BootstrapServers = "localhost:9092" };

            try
            {
                Action<DeliveryReportResult<Null, string>> handler = r =>
                Console.WriteLine(!r.Error.IsError
                    ? $"Delivered message to {r.TopicPartitionOffset}"
                    : $"Delivery Error: {r.Error.Reason}");

                using (var p = new Producer<Null, string>(conf))
                {
                    for (int i = 0; i < 12000; ++i)
                    {
                        p.BeginProduce("topico-teste", new Message<Null, string> { Value = i.ToString() }, handler);
                    }

                    // wait for up to 10 seconds for any inflight messages to be delivered.
                    p.Flush(TimeSpan.FromSeconds(10));
                }
            }
            catch (KafkaException ex)
            {
                throw ex;
            }

        }

    }
}