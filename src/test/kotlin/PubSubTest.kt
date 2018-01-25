import org.junit.AfterClass
import org.junit.Assert
import org.junit.Test

/**
 * Created by Matteo Gabellini on 25/01/2018.
 */


class PubSubTest{
    companion object {

        //Remember to start the RabbitMQ broker on the specified host
        // otherwise the system throw a ConnectionException
        private val brokerHost = "localhost"
        private val connector: BrokerConnector

        init{
            BrokerConnector.init(brokerHost)
            connector = BrokerConnector.INSTANCE
        }

        @AfterClass
        @JvmStatic
        fun closeConnection(){
            connector.channel.close()
            connector.connection.close()
        }
    }

    @Test
    fun singlePublish(){
        var subReceivedMessages = ArrayList<String>()


        var sub: Thread = Thread({
            val sub = Subscriber(connector, { X -> subReceivedMessages.add(X)})
            sub.subscribe(LifeParameters.HEART_RATE)
            println("Sono un testa di cazzo ma parto diavolo porco")
        })

        sub.start()

        Thread.sleep(2000)
        val pub: Thread = Thread( {
            val pub = Publisher(connector)
            pub.publish("Test 1", LifeParameters.HEART_RATE)
        })

        pub.start()

        Thread.sleep(10000);
        Assert.assertTrue(subReceivedMessages.size == 1)
    }

    @Test
    fun normalPublishingOnATopic(){
        var sub1ReceivedMessages = ArrayList<String>()
        var sub2ReceivedMessages = ArrayList<String>()

        val sub1Code:Runnable = Runnable {
            val sub = Subscriber(connector, { X -> sub1ReceivedMessages.add(X)})
            LifeParameters.values().forEach { X -> sub.subscribe(X) }
        }

        val sub2Code:Runnable = Runnable {
            val sub = Subscriber(connector, { X -> sub2ReceivedMessages.add(X)})
            LifeParameters.values().forEach { X -> sub.subscribe(X) }
        }

        var sub1: Thread = Thread(sub1Code)
        var sub2: Thread = Thread(sub2Code)

        sub1.start()
        sub2.start()

        Thread.sleep(2000)
        val pub: Thread = Thread( {
            val pub = Publisher(connector)
            for (i in 0..10) {
                pub.publish(i.toString(), LifeParameters.HEART_RATE)
            }
        })

        pub.start()

        Thread.sleep(5000);

        println(sub1ReceivedMessages.size)
        println(sub2ReceivedMessages.size)

        println("First")
        sub1ReceivedMessages.forEach({X -> print(X + " ")})
        println("\n Second")
        sub2ReceivedMessages.forEach({X -> print(X+ " ")})
        Assert.assertTrue(sub1ReceivedMessages.equals(sub2ReceivedMessages))
    }


    @Test
    fun normalPublishingOnAllTopic(){
        var sub1ReceivedMessages = HashSet<String>()
        var sub2ReceivedMessages = HashSet<String>()

        val sub1Code:Runnable = Runnable {
            val sub = Subscriber(connector, { X -> sub1ReceivedMessages.add(X)})
            LifeParameters.values().forEach { X -> sub.subscribe(X) }
        }

        val sub2Code:Runnable = Runnable {
            val sub = Subscriber(connector, { X -> sub2ReceivedMessages.add(X)})
            LifeParameters.values().forEach { X -> sub.subscribe(X) }
        }

        var sub1: Thread = Thread(sub1Code)
        var sub2: Thread = Thread(sub2Code)

        sub1.start()
        sub2.start()

        Thread.sleep(2000)
        val pub: Thread = Thread( {
            val pub = Publisher(connector)
            for (i in 0..10) {
                LifeParameters.values().forEach { X -> pub.publish(i.toString(), X) }
            }
        })

        pub.start()

        Thread.sleep(5000);

        println(sub1ReceivedMessages.size)
        println(sub2ReceivedMessages.size)

        println("First")
        sub1ReceivedMessages.forEach({X -> print(X + " ")})
        println("\n Second")
        sub2ReceivedMessages.forEach({X -> print(X+ " ")})
        Assert.assertTrue(sub1ReceivedMessages.equals(sub2ReceivedMessages))
    }
    @Test
    fun SubscriberUnsubscribing(){
        var subReceivedMessages = ArrayList<String>()

        var sub: Thread = Thread({
            val sub = Subscriber(connector, {
                X -> subReceivedMessages.add(X)
            })
            sub.subscribe(LifeParameters.HEART_RATE)
            Thread.sleep(3000)
            sub.unsubscribe(LifeParameters.HEART_RATE)
        })

        sub.start()

        Thread.sleep(2000)

        val pub: Thread = Thread( {
            val pub = Publisher(connector)
            for (i in 0..5) {
                pub.publish(i.toString(), LifeParameters.HEART_RATE)
                Thread.sleep(1000)
            }
        })

        pub.start()

        Thread.sleep(7000);
        print(subReceivedMessages.size)
        Assert.assertTrue(subReceivedMessages.size < 4)
    }


    @Test
    fun LateSubscribing() {
        val pub: Thread = Thread( {
            val pub = Publisher(connector)
            for (i in 0..10) {
                pub.publish(i.toString(), LifeParameters.HEART_RATE)
            }
        })

        pub.start()

        Thread.sleep(2000);

        var subReceivedMessages = ArrayList<String>()

        var sub: Thread = Thread({
            val sub = Subscriber(connector, {
                X -> subReceivedMessages.add(X)
            })
            sub.subscribe(LifeParameters.HEART_RATE)
        })

        sub.start()

        Thread.sleep(2000);

        Assert.assertTrue(subReceivedMessages.isEmpty())
    }

    @Test
    fun NotOverlappingSubscibing() {
        var sub1ReceivedMessages = ArrayList<String>()
        var sub2ReceivedMessages = ArrayList<String>()

        val sub1Code:Runnable = Runnable {
            val sub = Subscriber(connector, { X -> sub1ReceivedMessages.add(X)})
            sub.subscribe(LifeParameters.HEART_RATE)
            sub.subscribe(LifeParameters.DIASTOLIC_BLOOD_PRESSURE)
            sub.subscribe(LifeParameters.END_TIDAL_CARBON_DIOXIDE)
        }

        val sub2Code:Runnable = Runnable {
            val sub = Subscriber(connector, { X -> sub2ReceivedMessages.add(X)})
            sub.subscribe(LifeParameters.OXYGEN_SATURATION)
            sub.subscribe(LifeParameters.SYSTOLIC_BLOOD_PRESSURE)
            sub.subscribe(LifeParameters.TEMPERATURE)
        }

        var sub1: Thread = Thread(sub1Code)
        var sub2: Thread = Thread(sub2Code)

        sub1.start()
        sub2.start()

        Thread.sleep(3000);
        val pub: Thread = Thread( {
            val pub = Publisher(connector)
            for (i in 0..10) {
                LifeParameters.values().forEach {  X -> pub.publish(X.acronym + i.toString(), X) }
            }
        })

        pub.start()

        Thread.sleep(5000);

        println(sub1ReceivedMessages.size)
        Assert.assertTrue(sub1ReceivedMessages.size == 33)
        Assert.assertTrue(sub2ReceivedMessages.size == 33)

        sub1ReceivedMessages.forEach({X -> println(X)})
        sub2ReceivedMessages.forEach({X -> println(X)})
        Assert.assertFalse(sub1ReceivedMessages.equals(sub2ReceivedMessages))
    }

    @Test
    fun OverlappingSubscibing() {
        var sub1ReceivedMessages = ArrayList<String>()
        var sub2ReceivedMessages = ArrayList<String>()
        var overlappedElement = ArrayList<String>()

        val sub1Code:Runnable = Runnable {
            val sub = Subscriber(connector, { X -> sub1ReceivedMessages.add(X)})
            sub.subscribe(LifeParameters.HEART_RATE)
            sub.subscribe(LifeParameters.DIASTOLIC_BLOOD_PRESSURE)
            sub.subscribe(LifeParameters.END_TIDAL_CARBON_DIOXIDE)
        }

        val sub2Code:Runnable = Runnable {
            val sub = Subscriber(connector, { X -> sub2ReceivedMessages.add(X)})
            sub.subscribe(LifeParameters.HEART_RATE)
            sub.subscribe(LifeParameters.SYSTOLIC_BLOOD_PRESSURE)
            sub.subscribe(LifeParameters.TEMPERATURE)
        }

        var sub1: Thread = Thread(sub1Code)
        var sub2: Thread = Thread(sub2Code)

        sub1.start()
        sub2.start()

        Thread.sleep(3000);
        val pub: Thread = Thread( {
            val pub = Publisher(connector)
            for (i in 0..10) {
                LifeParameters.values().forEach {  X -> pub.publish(X.acronym + i.toString(), X) }
                overlappedElement.add(LifeParameters.HEART_RATE.acronym + i.toString())
            }
        })

        pub.start()

        Thread.sleep(5000);

        println(sub1ReceivedMessages.size)
        Assert.assertTrue(sub1ReceivedMessages.size == 33)
        Assert.assertTrue(sub2ReceivedMessages.size == 33)


        sub1ReceivedMessages.forEach({X -> println(X)})
        sub2ReceivedMessages.forEach({X -> println(X)})
        Assert.assertTrue(sub1ReceivedMessages.containsAll(overlappedElement) &&
                sub2ReceivedMessages.containsAll(overlappedElement))
    }
}