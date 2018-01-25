import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Consumer
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope
import java.io.PrintStream

/**
 * Created by Matteo Gabellini on 25/01/2018.
 */
class Subscriber(val connector: BrokerConnector, val messageHandler: (String) -> Any){

    var subscribedChannel: HashMap<String,String> = HashMap();

    val consumer: Consumer = object: DefaultConsumer(connector.channel) {
        @Throws(java.io.IOException::class)
        override fun handleDelivery(consumerTag:String, envelope: Envelope, properties: AMQP.BasicProperties, body:ByteArray) {
            val message = String(body, Charsets.UTF_8)
            messageHandler(message)
        }
    }

    fun subscribe(topic: LifeParameters) {
        val queueName: String = connector.getNewQueue()

        connector.channel.queueBind(queueName, topic.acronym,"")
        subscribedChannel.put(
                topic.acronym,
                connector.channel.basicConsume(queueName,true, consumer
                ))
    }

    fun unsubscribe(topic: LifeParameters){
        connector.channel.basicCancel(subscribedChannel.get(topic.acronym))
        subscribedChannel.remove(topic.acronym)
    }
}

fun main(argv: Array<String>){
    BrokerConnector.init("localhost")
    val sub = Subscriber(BrokerConnector.INSTANCE, { X -> println(X) })
    LifeParameters.values().forEach { X -> sub.subscribe(X) }
}