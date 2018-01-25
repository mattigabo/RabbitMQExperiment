/**
 * Created by Matteo Gabellini on 25/01/2018.
 */
import com.rabbitmq.client.*


@Throws(java.io.IOException::class,
        java.lang.InterruptedException::class)
fun main(argv: Array<String>) {
    val QUEUE_NAME: String = "hello";

    var factory: ConnectionFactory = ConnectionFactory();
    factory.setHost("localhost");
    val connection: Connection = factory.newConnection();
    val channel: Channel = connection.createChannel();

    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    val consumer: Consumer = object:DefaultConsumer(channel) {
        @Throws(java.io.IOException::class)
        override fun handleDelivery(consumerTag:String, envelope:Envelope, properties:AMQP.BasicProperties, body:ByteArray) {
            val message = String(body, Charsets.UTF_8)
            println(" [x] Received '" + message + "'")
        }
    }
    channel.basicConsume(QUEUE_NAME, true, consumer);
}
