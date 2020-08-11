import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

fun main() {
    val logger = LoggerFactory.getLogger("MyCustomLogger")
    val properties = Properties()
    val bootstrapServers = "localhost:9092"
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)

    val producer = KafkaProducer<String, String>(properties)

    val topicName = "custom_topic"
    repeat(200) {
        val message = "opa $it"
        val key = "id_$it"
        val producerRecord: ProducerRecord<String, String> = ProducerRecord(topicName, key, message)

        logger.info("Current key: $key")

        producer.send(producerRecord, Callback { metadata, exception ->
            run {
                if (exception == null) {
                    val logMessage = """
                    Received new metadata.
                    Topic: ${metadata.topic()}
                    Partition: ${metadata.partition()}
                    Offset: ${metadata.offset()}
                    Timestamp: ${metadata.timestamp()}
                """.trimIndent()
                    logger.info(logMessage)
                } else {
                    logger.error("Error while producing", exception)
                }
            }
        })

    }
//    producer.flush()

     producer.close() // Flush and close
}