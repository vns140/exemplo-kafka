using Confluent.Kafka;
using System;
using System.Threading;

namespace client.kafka
{
    public class Consumer
    {
        ConsumerConfig conf = new ConsumerConfig
        {
            GroupId = "consumidor-topico-teste",
            
            
            BootstrapServers = "localhost:9092",
            // Note: The AutoOffsetReset property determines the start offset in the event
            // there are not yet any committed offsets for the consumer group for the
            // topic/partitions of interest. By default, offsets are committed
            // automatically, so in this example, consumption will only start from the
            // eariest message in the topic 'my-topic' the first time you run the program.
            AutoOffsetReset = AutoOffsetResetType.Earliest
        };

        public void Work()
        {
            try
            {
                using (var c = new Consumer<Ignore, string>(conf))
                {
                    c.Subscribe("topico-teste");
                    

                    bool consuming = true;
                    // The client will automatically recover from non-fatal errors. You typically
                    // don't need to take any action unless an error is marked as fatal.
                    c.OnError += (_, e) => consuming = !e.IsFatal;

                    while (consuming)
                    {
                        try
                        {
                            var cr = c.Consume();
                            Console.WriteLine($"Consumed '{Thread.CurrentThread.ManagedThreadId}' message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }

                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    c.Close();
                }
            }
            catch (KafkaException ex)
            {

                throw ex;
            }

        }
    }
}