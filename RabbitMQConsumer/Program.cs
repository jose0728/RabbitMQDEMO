using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading;

namespace RabbitMQConsumer
{
    internal class Program
    {
        static void Main(string[] args)
        {
            TopicPattern(args);
        }

        /// <summary>
        /// 一对一模式
        /// 一对多模式（需要多开启几个消费者）
        /// </summary>
        /// <param name="args"></param>
        public static void OneToOnePattern(string[] args)
        {
            string queueName = "hello";

            //创建连接工厂
            ConnectionFactory factory = new ConnectionFactory
            {
                UserName = "admin",//用户名
                Password = "admin",//密码
                HostName = "10.1.11.127"//rabbitmq ip
            };

            //创建连接
            using (IConnection connection = factory.CreateConnection())
            {
                //创建通道
                using (IModel channel = connection.CreateModel())
                {
                    //创建消费者对象
                    EventingBasicConsumer consumer = new EventingBasicConsumer(channel);


                    //公平分发,不要同一时间给一个工作者发送多于一个消息。告诉Rabbit每次只能向消费者发送一条信息，在消费者未确认之前,不再向他发送信息
                    channel.BasicQos(0, 1, false);

                    Console.WriteLine("RabbitMQConsumer Start...");

                    //接收到消息事件
                    consumer.Received += (ch, ea) =>
                    {
                        Thread.Sleep((new Random().Next(1, 6)) * 1000);//随机等待,实现能者多劳。能者多劳是建立在手动确认基础上
                        string message = Encoding.Default.GetString(ea.Body.ToArray());

                        Console.WriteLine($"收到消息：{message}");
                        //返回消息确认
                        channel.BasicAck(ea.DeliveryTag, false);
                    };

                    //启动消费者 设置为手动应答消息
                    channel.BasicConsume(queueName, false, consumer);
                    Console.ReadKey();
                }
            }
        }

        /// <summary>
        /// FanoutPattern
        /// </summary>
        /// <param name="args"></param>
        public static void FanoutPattern(string[] args)
        {
            string exchangeName = "exchange1";
            string routeKey = "fanyu";

            //创建一个随机数,以创建不同的消息队列。如果是同一个消息队列，则就变成worker模式
            int random = new Random().Next(1, 1000);
            Console.WriteLine("Start" + random.ToString());

            //创建连接工厂
            ConnectionFactory factory = new ConnectionFactory
            {
                UserName = "admin",//用户名
                Password = "admin",//密码
                HostName = "10.1.11.127"//rabbitmq ip
            };

            //创建连接
            using (IConnection connection = factory.CreateConnection())
            {
                //创建通道
                using (IModel channel = connection.CreateModel())
                {
                    //声明交换机
                    channel.ExchangeDeclare(exchangeName, "fanout");

                    //消息队列名称
                    string queueName = exchangeName + "_" + random.ToString();
                    //声明队列
                    channel.QueueDeclare(queueName, false, false, false, null);

                    //交换机和队列绑定
                    channel.QueueBind(queueName, exchangeName, routeKey);

                    channel.BasicQos(0, 1, false);

                    //定义消费者
                    var consumer = new EventingBasicConsumer(channel);

                    //接收到消息事件
                    consumer.Received += (ch, ea) =>
                    {
                        string message = Encoding.Default.GetString(ea.Body.ToArray());
                        Console.WriteLine($"收到消息：{message}");
                        //返回消息确认
                        channel.BasicAck(ea.DeliveryTag, false);
                    };
                    //启动消费者 设置为手动应答消息
                    channel.BasicConsume(queueName, false, consumer);
                    Console.ReadKey();
                }
            }
        }

        /// <summary>
        /// DirectPattern
        /// </summary>
        /// <param name="args"></param>
        public static void DirectPattern(string[] args)
        {
            if (args.Length == 0) throw new ArgumentException("args");

            string exchangeName = "exchange2";

            //创建一个随机数,以创建不同的消息队列。如果是同一个消息队列，则就变成worker模式
            int random = new Random().Next(1, 1000);
            Console.WriteLine("Start" + random.ToString());

            //创建连接工厂
            ConnectionFactory factory = new ConnectionFactory
            {
                UserName = "admin",//用户名
                Password = "admin",//密码
                HostName = "10.1.11.127"//rabbitmq ip
            };

            //创建连接
            using (IConnection connection = factory.CreateConnection())
            {
                //创建通道
                using (IModel channel = connection.CreateModel())
                {
                    //声明交换机
                    channel.ExchangeDeclare(exchangeName, "direct");

                    //消息队列名称
                    string queueName = exchangeName + "_" + random.ToString();
                    //声明队列
                    channel.QueueDeclare(queueName, false, false, false, null);

                    //将队列与交换机进行绑定
                    foreach (var routeKey in args)
                    {
                        //匹配多个路由
                        channel.QueueBind(queueName, exchangeName, routeKey);
                    }

                    channel.BasicQos(0, 1, false);

                    //定义消费者
                    var consumer = new EventingBasicConsumer(channel);

                    //接收到消息事件
                    consumer.Received += (ch, ea) =>
                    {
                        string message = Encoding.Default.GetString(ea.Body.ToArray());
                        Console.WriteLine($"收到消息：{message}");
                        //返回消息确认
                        channel.BasicAck(ea.DeliveryTag, false);
                    };
                    //启动消费者 设置为手动应答消息
                    channel.BasicConsume(queueName, false, consumer);
                    Console.ReadKey();
                }
            }
        }

        /// <summary>
        /// TopicPattern
        /// </summary>
        /// <param name="args"></param>
        /// <exception cref="ArgumentException"></exception>
        public static void TopicPattern(string[] args)
        {
            if (args.Length == 0) throw new ArgumentException("args");

            string exchangeName = "exchange3";

            //创建一个随机数,以创建不同的消息队列。如果是同一个消息队列，则就变成worker模式
            int random = new Random().Next(1, 1000);
            Console.WriteLine("Start" + random.ToString());

            //创建连接工厂
            ConnectionFactory factory = new ConnectionFactory
            {
                UserName = "admin",//用户名
                Password = "admin",//密码
                HostName = "10.1.11.127"//rabbitmq ip
            };

            //创建连接
            using (IConnection connection = factory.CreateConnection())
            {
                //创建通道
                using (IModel channel = connection.CreateModel())
                {
                    //声明交换机
                    channel.ExchangeDeclare(exchangeName, "topic");

                    //消息队列名称
                    string queueName = exchangeName + "_" + random.ToString();
                    //声明队列
                    channel.QueueDeclare(queueName, false, false, false, null);

                    //将队列与交换机进行绑定
                    foreach (var routeKey in args)
                    {
                        //匹配多个路由
                        channel.QueueBind(queueName, exchangeName, routeKey);
                    }

                    channel.BasicQos(0, 1, false);

                    //定义消费者
                    var consumer = new EventingBasicConsumer(channel);

                    //接收到消息事件
                    consumer.Received += (ch, ea) =>
                    {
                        string message = Encoding.Default.GetString(ea.Body.ToArray());
                        Console.WriteLine($"收到消息：{message}");
                        Console.WriteLine($"ea:{JsonConvert.SerializeObject(ea)}");
                        //返回消息确认
                        channel.BasicAck(ea.DeliveryTag, false);//deliveryTag:该消息的index；multiple：是否批量，为true:将一次性ack所有小于deliveryTag的消息。
                    };
                    //启动消费者 设置为手动应答消息
                    channel.BasicConsume(queueName, false, consumer);
                    Console.ReadKey();
                }
            }
        }
        

    }
}
