package Lib

import java.io.File
import java.util.Properties

import akka.actor.{Actor, Props}
import com.github.tototoshi.csv.CSVReader
import org.apache.kafka.clients.producer._

case object Tick

class Sender(dataSources: List[DataSource], kafkaProps: Properties) extends Actor{
  private val linesCount = 1
  val linesRead = 1
  val producer = new KafkaProducer[String, String](kafkaProps)
  override def receive: Receive = onMessage(linesCount)

  private def onMessage(linesCount: Int): Receive = {
    case Tick =>
      for (dataSource <- dataSources) {
        dataSource match {
          case FileSource(filePath, currencyPath, kafkaTopic) =>
            //Readers initialization
            val dataReader = CSVReader.open(new File(filePath))
            val currencyReader = CSVReader.open(new File(currencyPath))

            //Streams initialization
            val dataStream = dataReader.toStream
            val currencyStream = currencyReader.toStream

            //Data reading
            val articleData = dataStream.slice(linesCount, linesCount + linesRead).head.mkString(" ")
            val currencyData = currencyStream.slice(linesCount, linesCount + linesRead).head.tail.mkString(" ")
            val sendData = s"$articleData $currencyData"


            context.become(onMessage(linesCount + linesRead))

            //Data sending
            println(s"About to publish message '$sendData' to $kafkaTopic")
            val record = new ProducerRecord[String, String](kafkaTopic, "key", sendData)
            producer.send(record)
        }
      }
  }
}

object Sender {
  def props(data: List[DataSource], kafkaProps: Properties): Props = Props(new Sender(data, kafkaProps))
}
