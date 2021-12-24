using RabbitMQ.Client;
using System;
using System.Text;

namespace RabbitMQProducer
{
    /// <summary>
    /// RabbitMQ从信息接收者角度可以看做三种模式:
    /// 一对一
    /// 一对多(此一对多并不是发布订阅，而是每条信息只有一个接收者)
    /// 发布订阅
    /// 其中一对一是简单队列模式，一对多是Worker模式，而发布订阅包括发布订阅模式，路由模式和通配符模式
    /// </summary>
    internal class Program
    {
        static void Main(string[] args)
        {
            TopicPattern(args);
        }

        /// <summary>
        /// 一对一模式
        /// 一对多模式/worker模式（需要多开启几个消费者）
        /// </summary>
        /// <param name="args"></param>
        public static void OneToOnePattern(string[] args)
        {
            //创建连接工厂
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                UserName = "admin",
                Password = "admin",
                HostName = "10.1.11.127",
            };

            //创建连接
            using (IConnection connection = connectionFactory.CreateConnection())
            {
                //创建通道
                using (IModel channel = connection.CreateModel())
                {
                    //声明一个队列
                    channel.QueueDeclare("hello", false, false, false, null);

                    Console.WriteLine("RabbitMQ连接成功，请输入消息，输入exit退出！");

                    string input;
                    do
                    {
                        input = Console.ReadLine();
                        byte[] sendBytes = Encoding.UTF8.GetBytes(input);
                        //消息持久化
                        IBasicProperties props = channel.CreateBasicProperties();
                        props.Persistent = true;
                        props.DeliveryMode = 2;
                        //发布消息
                        channel.BasicPublish("", "hello", props, sendBytes);
                        Console.WriteLine("成功发送消息:" + input);
                    }
                    while (input.Trim().ToLower() != "exit");
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

            //创建连接工厂
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                UserName = "admin",
                Password = "admin",
                HostName = "10.1.11.127",
            };

            //创建连接
            using (IConnection connection = connectionFactory.CreateConnection())
            {
                //创建通道
                using (IModel channel = connection.CreateModel())
                {
                    //声明交换机
                    channel.ExchangeDeclare(exchangeName,"fanout");

                    Console.WriteLine("RabbitMQ连接成功，请输入消息，输入exit退出！");

                    string input;
                    do
                    {
                        input = Console.ReadLine();
                        byte[] sendBytes = Encoding.UTF8.GetBytes(input);
                        //发布消息
                        channel.BasicPublish(exchangeName, "fanyu", null, sendBytes);
                        Console.WriteLine("成功发送消息:" + input);
                    }
                    while (input.Trim().ToLower() != "exit");
                }
            }
        }

        /// <summary>
        /// DirectPattern
        /// </summary>
        /// <param name="args"></param>
        public static void DirectPattern(string[] args)
        {
            string exchangeName = "exchange2";

            //创建连接工厂
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                UserName = "admin",
                Password = "admin",
                HostName = "10.1.11.127",
            };

            //创建连接
            using (IConnection connection = connectionFactory.CreateConnection())
            {
                //创建通道
                using (IModel channel = connection.CreateModel())
                {
                    //声明交换机
                    channel.ExchangeDeclare(exchangeName, "direct");

                    Console.WriteLine("RabbitMQ连接成功，请输入消息，输入exit退出！");

                    string input;
                    do
                    {
                        input = Console.ReadLine();
                        byte[] sendBytes = Encoding.UTF8.GetBytes(input);
                        //发布消息
                        channel.BasicPublish(exchangeName, "fanyu", null, sendBytes);
                        Console.WriteLine("成功发送消息:" + input);
                    }
                    while (input.Trim().ToLower() != "exit");
                }
            }
        }

        /// <summary>
        /// TopicPattern
        /// </summary>
        /// <param name="args"></param>
        public static void TopicPattern(string[] args)
        {
            string exchangeName = "exchange3";

            //创建连接工厂
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                UserName = "admin",
                Password = "admin",
                HostName = "10.1.11.127",
            };

            //创建连接
            using (IConnection connection = connectionFactory.CreateConnection())
            {
                //创建通道
                using (IModel channel = connection.CreateModel())
                {
                    //声明交换机
                    channel.ExchangeDeclare(exchangeName, "topic");

                    Console.WriteLine("RabbitMQ连接成功，请输入消息，输入exit退出！");

                    string input;
                    do
                    {
                        input = Console.ReadLine();
                        byte[] sendBytes = Encoding.UTF8.GetBytes(input);
                        //发布消息
                        channel.BasicPublish(exchangeName, "fanyu.ayu.#", null, sendBytes);
                        Console.WriteLine("成功发送消息:" + input);
                    }
                    while (input.Trim().ToLower() != "exit");
                }
            }
        }
    }
}
