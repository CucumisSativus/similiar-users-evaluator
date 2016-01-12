package tul.poiis.similar_users_evaluator

import java.io._

import com.github.tototoshi.csv._
/**
  * Created by michal on 16.12.2015.
  */
object MyCsvReader {
  implicit object MyCsvReader extends DefaultCSVFormat {
    override val delimiter = ';'
  }

  def reader(filePath: String) = CSVReader.open(new File(filePath))
}

