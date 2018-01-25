/**
 * Created by Matteo Gabellini on 25/01/2018.
 */
class Publisher(val connector: BrokerConnector) {

    fun publish(message:String, topic: LifeParameters) {
        connector.channel.basicPublish(topic.acronym,"", null, message.toByteArray(charset("UTF-8")))
        println(" [x] Sent '$message' on '${topic.acronym}' ")

    }
}

fun main(argv: Array<String>){
    BrokerConnector.init("localhost")
    val pub = Publisher(BrokerConnector.INSTANCE)
    //LifeParameters.values().forEach { X -> pub.publish("Stampo su "+ X.longName, X) }
    for (i in 0..10) {
        pub.publish(i.toString(), LifeParameters.HEART_RATE)
    }
}