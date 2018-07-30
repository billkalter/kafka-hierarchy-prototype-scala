package com.bazaarvoice.legion.hierarchy

import java.util.concurrent.{ExecutionException, Executors, TimeUnit}
import java.util.{Collections, Properties}

import com.bazaarvoice.legion.hierarchy.model.Lineage
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.collection.JavaConverters
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object Runner {
  val _customBrokerProperties: Map[String, String] = Map(
    "broker.id" -> "0",
    "num.network.threads" -> "3",
    "num.io.threads" -> "8",
    "num.partitions" -> "1"
  )

  def createStreamsConfig(bootstrapServers: String): StreamsConfig = {
    val settings = new Properties
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "category-hierarchy-prototype-" + System.currentTimeMillis)
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    settings.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1")
    settings.put(StreamsConfig.POLL_MS_CONFIG, "100")
    settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500")
    settings.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all")
    new StreamsConfig(settings)
  }

  def createAdminClient(bootstrapServers: String): AdminClient = {
    val props = new Properties
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    AdminClient.create(props)
  }

  def createTopics(adminClient: AdminClient, hierarchyStreamConfig: HierarchyStreamConfig): Unit = {
    val topics = JavaConverters asJavaCollection Map(
      hierarchyStreamConfig.sourceTopic -> TopicConfig.CLEANUP_POLICY_COMPACT,
      hierarchyStreamConfig.parentTransitionTopic -> TopicConfig.CLEANUP_POLICY_COMPACT,
      hierarchyStreamConfig.childTransitionTopic -> TopicConfig.CLEANUP_POLICY_DELETE,
      hierarchyStreamConfig.parentChildrenTopic -> TopicConfig.CLEANUP_POLICY_COMPACT,
      hierarchyStreamConfig.destTopic -> TopicConfig.CLEANUP_POLICY_COMPACT
    ).map((t: (String, String)) => {
      val (topicName, compaction) = t
      val topic = new NewTopic(topicName, 1, 1.toShort)
      topic.configs(JavaConverters mapAsJavaMap Map(TopicConfig.CLEANUP_POLICY_CONFIG -> compaction))
      topic
    }).toSeq

    val results = adminClient.createTopics(topics)

    JavaConverters mapAsScalaMap results.values() foreach ((t: (String, KafkaFuture[Void])) => {
      val (topic, future) = t
      try {
        future.get()
      } catch {
        case e: ExecutionException => {
          if (!e.getCause.isInstanceOf[TopicExistsException]) {
            // TODO:  Log the topic
            throw e.getCause
          }
        }
      }
    })
  }

  def pollDestinationTopic(service: ExecutionContextExecutorService, destTopic: String, bootstrapServers: String): Unit = {
    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "FinalDestinationConsumer")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[HierarchySerdes.LineageDeserializer].getName)
    val consumer = new KafkaConsumer[String, Lineage](props)
    consumer.subscribe(Collections.singleton(destTopic))

    service.execute(() => {
      while (!service.isShutdown) {
        try {
          val records = consumer.poll(500)
          (JavaConverters iterableAsScalaIterable records) foreach (record => {
            // TODO:  Use logger
            println(s"ID ${record.key} has parents ${record.value().parents}")
          })
        } catch {
          case e: Exception => {
            // TODO:  Use logger
            println(s"Failed to poll: $e")
          }
        }
      }
    })
  }

  def main(args: Array[String]): Unit = {
    implicit val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2181, customBrokerProperties = _customBrokerProperties)
    val bootstrapServers = "localhost:" + embeddedKafkaConfig.kafkaPort

    EmbeddedKafka.withRunningKafka {
      val adminClient = createAdminClient(bootstrapServers)
      try {
        val hierarchyStreamConfig = HierarchyStreamConfig("source", "child-parent-transition", "parent-child-transition", "children", "destination")

        createTopics(adminClient, hierarchyStreamConfig)
        val streams = new KafkaStreams(TopologyGenerator(hierarchyStreamConfig), createStreamsConfig(bootstrapServers))
        streams.start()
        println("Streams started")

        val service = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool)
        pollDestinationTopic(service, hierarchyStreamConfig.destTopic, bootstrapServers)

        val props = new Properties
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        props.put(ProducerConfig.ACKS_CONFIG, "all")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

        val producer = new KafkaProducer[String, String](props)

        producer.send(new ProducerRecord[String, String](hierarchyStreamConfig.sourceTopic, "animal", null)).get()
        producer.send(new ProducerRecord[String, String](hierarchyStreamConfig.sourceTopic, "dog", "animal")).get()
        producer.send(new ProducerRecord[String, String](hierarchyStreamConfig.sourceTopic, "cat", "animal")).get()
        producer.send(new ProducerRecord[String, String](hierarchyStreamConfig.sourceTopic, "fido", "dog")).get()
        producer.send(new ProducerRecord[String, String](hierarchyStreamConfig.sourceTopic, "animal", "__root__")).get()

        while (!service.isShutdown) {
          service.awaitTermination(1, TimeUnit.SECONDS)
        }
      } finally adminClient.close()
    }
  }
}
