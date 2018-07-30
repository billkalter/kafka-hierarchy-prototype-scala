package com.bazaarvoice.legion.hierarchy

import java.util.concurrent.{ExecutionException, Executors}
import java.util.{Collections, Properties}

import com.bazaarvoice.legion.hierarchy.model.Lineage
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.collection.JavaConverters
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object Runner {
  def main(args: Array[String]): Unit = {
    val embeddedKafkaConfig = EmbeddedKafkaConfig(9092, 2181, customBrokerProperties = _customBrokerProperties)
    val boostrapServers = "localhost:" + embeddedKafkaConfig.kafkaPort

    EmbeddedKafka withRunningKafka {
      val adminClient = createAdminClient(boostrapServers)
      try {
        val hierarchyStreamConfig = HierarchyStreamConfig("source", "child-parent-transition", "parent-child-transition", "children", "destination")

        createTopics(adminClient, hierarchyStreamConfig)
        val streams = new KafkaStreams(TopologyGenerator(hierarchyStreamConfig), createStreamsConfig(boostrapServers))
        streams.start();
        println("Streams started")

        val service = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool)
        pollDestinationTopic(service, hierarchyStreamConfig.destTopic, boostrapServers)



      } finally adminClient.close()
    }
  }

  val _customBrokerProperties : Map[String, String] = Map(
    "broker.id" -> "0",
    "num.network.threads" -> "3",
    "num.io.threads" -> "8",
    "num.partitions" -> "1"
  )

  def createStreamsConfig(bootstrapServers : String) : StreamsConfig = {
    val settings = new Properties
    settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "category-hierarchy-prototype-" + System.currentTimeMillis)
    settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    settings.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "1")
    settings.put(StreamsConfig.POLL_MS_CONFIG, "100")
    settings.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500")
    settings.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all")
    new StreamsConfig(settings)
  }

  def createAdminClient(bootstrapServers : String) : AdminClient = {
    val props = new Properties
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    AdminClient.create(props)
  }

  def createTopics(adminClient : AdminClient, hierarchyStreamConfig: HierarchyStreamConfig): Unit = {
    val topics = JavaConverters asJavaCollection Map(
      hierarchyStreamConfig.sourceTopic -> TopicConfig.CLEANUP_POLICY_COMPACT,
      hierarchyStreamConfig.parentTransitionTopic -> TopicConfig.CLEANUP_POLICY_COMPACT,
      hierarchyStreamConfig.childTransitionTopic -> TopicConfig.CLEANUP_POLICY_DELETE,
      hierarchyStreamConfig.parentChildrenTopic -> TopicConfig.CLEANUP_POLICY_COMPACT,
      hierarchyStreamConfig.destTopic -> TopicConfig.CLEANUP_POLICY_COMPACT
    ).map((t : (String, String)) => {
      val (topicName, compaction) = t
      val topic = new NewTopic(topicName, 1, 1.toShort)
      topic.configs(JavaConverters mapAsJavaMap Map(TopicConfig.CLEANUP_POLICY_CONFIG -> compaction))
      topic
    }).toSeq

    val results = adminClient.createTopics(topics)

    JavaConverters mapAsScalaMap results.values() foreach ((t : (String, KafkaFuture[Void])) => {
      val (topic, future) = t
      try {
        future.get()
      } catch {
        case e : ExecutionException => {
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
          case e : Exception => {
            // TODO:  Use logger
            println(s"Failed to poll: $e")
          }
        }
      }
    })
  }
}
