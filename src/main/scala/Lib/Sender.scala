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

  private def fileDataSend(fileSource: FileSource, linesCount: Int): Unit ={
    //Readers initialization
    val dataReader = CSVReader.open(new File(fileSource.filePath))
    val currencyReader = CSVReader.open(new File(fileSource.currencyPath))

    //Streams initialization
    val dataStream = dataReader.toStream
    val currencyStream = currencyReader.toStream

    //Data reading
    if(linesCount != 1) {
      val tuneDataPart2 = dataStream
        .slice(linesCount - 1, linesCount - 1 + linesRead)
        .head
        .tail
        .mkString(" ")
      val Stream(tuneDataPart1, predictData) = currencyStream
        .slice(linesCount - 1, linesCount + linesRead)
        .map(list => list.mkString(" "))

      val tuneData = s"$tuneDataPart1 $tuneDataPart2"

      val tuneTopic = s"${fileSource.kafkaTopic}_tune"
      val predictTopic = fileSource.kafkaTopic

      //Data sending
      println(s"About to publish message '$tuneData' to $tuneTopic")
      println(s"About to publish message '$predictData' to $predictTopic")
      println()
      val tuneRecord = new ProducerRecord[String, String](tuneTopic, "key", tuneData)
      val predictRecord = new ProducerRecord[String, String](predictTopic, "key", predictData)
      producer.send(tuneRecord)
      producer.send(predictRecord)
    }
  }

  private def onMessage(linesCount: Int): Receive = {
    case Tick =>
      for (dataSource <- dataSources) {
        dataSource match {
          case fileSource: FileSource =>
            fileDataSend(fileSource, linesCount)
            context.become(onMessage(linesCount + linesRead))
        }
      }
  }
}

object Sender {
  def props(data: List[DataSource], kafkaProps: Properties): Props = Props(new Sender(data, kafkaProps))
}
