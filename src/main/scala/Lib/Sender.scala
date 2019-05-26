package Lib

import java.io.{BufferedWriter, File, FileWriter, InputStream}
import java.util.Properties

import akka.actor.{Actor, Props}
import com.github.tototoshi.csv.CSVReader
import org.apache.kafka.clients.producer._

case object Tick

class Sender(dataSources: List[DataSource], kafkaProps: Properties) extends Actor{
  private val linesCount = 1
  val linesRead = 1
  override def receive: Receive = onMessage(linesCount)

  private def fileDataSend(fileSource: FileSource, linesCount: Int): Unit ={
    //Data reading
    if(linesCount != 1) {
      if(linesCount > 105)
        println("End")
      else {
        val producer = new KafkaProducer[String, String](kafkaProps)

        //Readers initialization
        val dataReader = CSVReader.open(new File(fileSource.filePath))
        val currencyReader = CSVReader.open(new File(fileSource.currencyPath))

        //Streams initialization
        val dataStream = dataReader.toStream
        val currencyStream = currencyReader.toStream

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
        /*println(s"About to publish message '$tuneData' to $tuneTopic")
        println(s"About to publish message '$predictData' to $predictTopic")
        println()*/
        val tuneRecord = new ProducerRecord[String, String](tuneTopic, "key", tuneData)
        val predictRecord = new ProducerRecord[String, String](predictTopic, "key", predictData)
        producer.send(tuneRecord)
        producer.send(predictRecord)
      }
    }
  }

  private def onMessage(linesCount: Int): Receive = {
    case Tick =>
      val commonStart = System.currentTimeMillis()
      dataSources.par.zip(1 to 3).foreach{
        case (fileSource: FileSource, number: Int) =>
          println(Thread.currentThread().getId)
          val localStart = System.currentTimeMillis()
          fileDataSend(fileSource, linesCount)
          val localEnd = System.currentTimeMillis()

          val bw = new BufferedWriter(new FileWriter(s"SenderTest2_$number-c.csv", true))
          bw.write(s"$localStart, $localEnd, ${localEnd - localStart}, ${localStart - commonStart}, ${localEnd - commonStart}")
          bw.newLine()
          bw.close()
          //if(linesCount < 105)
          context.become(onMessage(linesCount + linesRead))
      }
      /*for (dataSource <- dataSources) {
        dataSource match {
          case fileSource: FileSource =>
            fileDataSend(fileSource, linesCount)
        }
      }*/
      println(linesCount)
      println()
      /*val end = System.currentTimeMillis()
      val bw = new BufferedWriter(new FileWriter("SenderTest2.csv", true))
      bw.write(s"$commonStart, $end, ${end - commonStart}")
      bw.newLine()
      bw.close()*/
      //if(linesCount < 105)
      context.become(onMessage(linesCount + linesRead))
      /*else
      context.become(onMessage(1))*/
  }
}

object Sender {
  def props(data: List[DataSource], kafkaProps: Properties): Props = Props(new Sender(data, kafkaProps))
}
