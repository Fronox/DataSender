package Lib

trait DataSource
case class FileSource(filePath: String, currencyPath: String, kafkaTopic: String) extends DataSource
case class ApiSource(url: String, kafkaTopic: String) extends DataSource
