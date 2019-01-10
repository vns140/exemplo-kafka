using System;

namespace client.kafka
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            Producer producer = new Producer();
            producer.PublishAsync();


            Consumer consumer = new Consumer();
            consumer.Work();
        }
    }
}
