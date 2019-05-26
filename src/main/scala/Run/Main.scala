package Run

import java.io.File
import java.util.Properties

import Lib.{DataSource, FileSource, Sender, Tick}
import akka.actor.ActorSystem
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.io.Source

object Main {
  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
  val scheduler: QuartzSchedulerExtension = QuartzSchedulerExtension(actorSystem)
  val props = new Properties()
  //val jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.getPath).getParentFile.getAbsolutePath
  val configPath = "config.json"  //jarPath + "/config.json"

  def main(args: Array[String]): Unit = {
    //Properties for kafka
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //Reading config file
    val configFile = Source.fromFile(configPath)
    val configJson = parse(configFile.mkString)
    val sourceInfoList = (configJson \\ "sources").extract[List[String]]
    val sources: List[DataSource] = for (sourceInfo <- sourceInfoList) yield {
      val sourceJson = configJson \\ sourceInfo
      val sourceType = (sourceJson \\ "source_type").extract[String]
      sourceType match {
        case "file" =>
          val filePath = (sourceJson \\ "fname").extract[String]
          val currencyPath = (sourceJson \\ "currency_file").extract[String]

          //val filePath = jarPath + "/" + (sourceJson \\ "fname").extract[String]
          //val currencyPath = jarPath + "/" + (sourceJson \\ "currency_file").extract[String]

          FileSource(filePath, currencyPath, sourceInfoList.head/*Todo: change to sourceInfo*/)
        // For further extensions
        /*case "api" =>
          ???
        case _ =>
          println("Unknown source type")*/
      }
    }
    configFile.close()

    val secondsCount = 1
    val sender = actorSystem.actorOf(Sender.props(sources, props), "sender")
    val scheduleName = s"Every${secondsCount}Second(s)"
    val scheduleParams = s"*/$secondsCount * * ? * *"
    scheduler.createSchedule(scheduleName, None, scheduleParams)
    scheduler.schedule(scheduleName, sender, Tick)
  }
}
