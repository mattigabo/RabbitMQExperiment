/**
 * Created by Matteo Gabellini on 24/01/2018.
 */
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.Connection
import com.rabbitmq.client.Channel


@Throws(Exception::class)
fun main(argv: Array<String>) {
    val QUEUE_NAME: String = "hello"
    val factory: ConnectionFactory = ConnectionFactory()
    factory.host = "localhost"
    val connection: Connection = factory.newConnection()
    val channel: Channel = connection.createChannel()

    channel.queueDeclare(QUEUE_NAME, false, false, false, null)
    for (i in 0..10) {
        val message = "Hello World!" + i
        channel.basicPublish("", QUEUE_NAME, null, message.toByteArray(charset("UTF-8")))
        println(" [x] Sent '$message'")
    }


    channel.close()
    connection.close()
}