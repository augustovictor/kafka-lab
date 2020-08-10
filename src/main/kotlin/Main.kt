import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

fun main() {
    val properties = Properties()
    val bootstrapServers = "localhost:9092"
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.canonicalName)

    val producer: KafkaProducer<String, String> = KafkaProducer(properties)

    val topicName = "custom_topic"
    val message = "opa"
    val producerRecord: ProducerRecord<String, String> = ProducerRecord(topicName, message)

    producer.send(producerRecord)
    producer.flush()

    // producer.close() // Flush and close
}