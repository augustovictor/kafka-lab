import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

fun main() {
    val logger = LoggerFactory.getLogger("ConsumerLogger")

    val bootstrapServer = "localhost:9092"
    val groupId = "kafka-lab-consumer"
    val autoOffsetReset = "earliest"
    val topicName = "custom_topic"

    val properties = Properties()
    properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServer
    properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.canonicalName
    properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.canonicalName
    properties[ConsumerConfig.GROUP_ID_CONFIG] = groupId
    properties[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = autoOffsetReset

    val consumer = KafkaConsumer<String, String>(properties)

    consumer.subscribe(Collections.singleton(topicName)) // Collections.singleton(...) means we're subscribing to one topic only

    while (true) {
        val consumerRecords = consumer.poll(Duration.ofMillis(100))

        consumerRecords.iterator().forEach {
            val logMessage = """
                    Received new metadata.
                    Topic: ${it.topic()}
                    Partition: ${it.partition()}
                    Offset: ${it.offset()}
                    Timestamp: ${it.timestamp()}
                """.trimIndent()
            logger.info(logMessage)
        }
    }
}
