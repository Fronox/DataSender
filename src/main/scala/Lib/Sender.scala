package Lib

import java.io.File
import java.util.Properties

import akka.actor.{Actor, Props}
import com.github.tototoshi.csv.CSVReader
import org.apache.kafka.clients.producer._

case object Tick

class Sender(dataSources: List[DataSource], kafkaProps: Properties) extends Actor {
  private val linesCount = 1
  val linesRead = 15000

  val maxLinesCount = 25
  override def receive: Receive = onMessage(linesCount)

  private def fileDataSend(fileSource: FileSource, linesCount: Int, key: Int): Unit ={
    //Data reading
    if(linesCount > maxLinesCount)
      println("End")
    else {
      val producer = new KafkaProducer[String, String](kafkaProps)

      //Readers initialization
      val dataReader = CSVReader.open(new File(fileSource.filePath))
      val currencyReader = CSVReader.open(new File(fileSource.currencyPath))
      val tuneTopic = s"${fileSource.kafkaTopic}_tune"
      val predictTopic = fileSource.kafkaTopic

      //Streams initialization
      val dataStream = dataReader.toStream
      val currencyStream = currencyReader.toStream

      val tuneDataPart2 = dataStream
        .slice(linesCount, linesCount - 1 + linesRead)
        .map(x => x.tail.mkString(" ")) :+ ""

      currencyStream
        .slice(linesCount, linesCount + linesRead)
        .map(list => list.mkString(" "))
        .zip(tuneDataPart2)
        .zipWithIndex
        .foreach{
          case ((data1, data2), i) =>
            val tuneData = data1 + " " + (if(data2 == "") "1" else data2)
            val predictData = data1
            //Data sending
            println(s"About to publish message '$tuneData' to $tuneTopic")
            val tuneRecord = new ProducerRecord[String, String](tuneTopic, s"$key", tuneData)
            producer.send(tuneRecord)

            println(s"About to publish message '$predictData' to $predictTopic")
            val predictRecord = new ProducerRecord[String, String](predictTopic, s"$key", predictData)
            producer.send(predictRecord)
        }
    }
  }
  //On message function
  private def onMessage(linesCount: Int): Receive = {
    case Tick =>
      dataSources.zipWithIndex.par.foreach {
        case (fileSource: FileSource, number: Int) =>
          fileDataSend(fileSource, linesCount, number)
      }
      println(linesCount)
      println()
      if((linesCount + linesRead) < maxLinesCount)
        context.become(onMessage(linesCount + linesRead))
      else
        context.become(onMessage(1))
  }
}

object Sender {
  def props(data: List[DataSource], kafkaProps: Properties): Props = Props(new Sender(data, kafkaProps))
}
