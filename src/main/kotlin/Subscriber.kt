import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Consumer
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope

/**
 *
 * Wrapper of a Subscriber for RabbitMq Broker
 *
 * Created by Matteo Gabellini on 25/01/2018.
 */
class Subscriber(val connector: BrokerConnector) {

    private var subscribedChannel: HashMap<String, String> = HashMap()

    /**
     *
     *  Do the subscription to the specified channel
     *
     *  @topic the
     * */
    fun subscribe(topic: LifeParameters, consumingLogic: Consumer) {
        val queueName: String = connector.getNewQueue()

        connector.channel.queueBind(queueName, topic.acronym,"")
        subscribedChannel.put(
                topic.acronym,
                connector.channel.basicConsume(queueName, true, consumingLogic
                ))
    }

    fun unsubscribe(topic: LifeParameters){
        connector.channel.basicCancel(subscribedChannel.get(topic.acronym))
        subscribedChannel.remove(topic.acronym)
    }

    /**
     * Return a defence copy of the current subscribed channel.
     *
     * In order to change a subscription to a channel use subscribe/unsubscribe methods
     * */
    fun subscribedChannel() = HashMap<String, String>().putAll(this.subscribedChannel)

    fun createStringConsumer(messageHandler: (String) -> Any): Consumer {
    return object : DefaultConsumer(connector.channel) {
            @Throws(java.io.IOException::class)
            override fun handleDelivery(consumerTag: String,
                                        envelope: Envelope,
                                        properties: AMQP.BasicProperties,
                                        body: ByteArray) {
                val message = String(body, Charsets.UTF_8)
                messageHandler(message)
            }
        }
    }
}

fun main(argv: Array<String>){
    BrokerConnector.init("localhost")
    val sub = Subscriber(BrokerConnector.INSTANCE)

    val consumer = sub.createStringConsumer { X ->
        println(X)
    }
    LifeParameters.values().forEach { X -> sub.subscribe(X, consumer) }
    //LifeParameters.values().forEach { X -> sub.unsubscribe(X) }
    //BrokerConnector.INSTANCE.close()
}