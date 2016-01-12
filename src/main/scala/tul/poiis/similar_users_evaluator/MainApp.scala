package tul.poiis.similar_users_evaluator

import java.io.{File, PrintWriter}

import tul.poiis.similar_users_evaluator.Evaluation.MovieId

import scala.math

object MainApp extends App {

  def readMovies(filepath: String): Set[MovieId] ={
    val reader = MyCsvReader.reader(filepath)
    val csv_list = reader.allWithHeaders()
    reader.close()
    csv_list.map { entry =>
      entry("Id").toInt
    }.toSet

  }
  def parseTrainEntry(csv_entry: List[String]): (Int, Evaluation) ={
    val personId = csv_entry(1)
    val movieId = csv_entry(2)
    val grade = csv_entry(3)
    (personId.toInt, Evaluation(grade = Some(grade.toInt), movieId = movieId.toInt))
  }

  def readTrainSetFile(filepath: String, movies: Set[MovieId]): Map[Int, List[Evaluation]] ={
    val reader = MyCsvReader.reader(filepath)
    val csv_list: List[List[String]] = reader.all()
    reader.close()
    val parsedTuples = csv_list.par.map { entry =>
      val parseResult: (Int, Evaluation) = parseTrainEntry(entry)
      parseResult._1 -> parseResult._2
    }.seq.toList
    val map = parsedTuples.groupBy(_._1).mapValues(_.map(_._2))
    map.mapValues{ evaluations =>
     val gradedMovies = evaluations.map(_.movieId).toSet[MovieId]
      val notGradedMoviesEvaluations = (movies -- gradedMovies).map(m => Evaluation(movieId = m, grade = None))
      evaluations ++ notGradedMoviesEvaluations
    }

  }

  def parseUnknownEntry(csv_entry: List[String]): (Int, Int, Int) ={
    val evalId = csv_entry(0)
    val personId = csv_entry(1)
    val movieId = csv_entry(2)
    (evalId.toInt, personId.toInt, movieId.toInt)
  }

  def readUnknowns(filepath: String): List[(Int, Int, Int)] ={
    val reader = MyCsvReader.reader(filepath)
    val csv_list: List[List[String]] = reader.all()
    reader.close()
    csv_list.map { entry =>
      parseUnknownEntry(entry)
    }
  }

  def calculateOptionPartialError(value1: Option[Int], value2: Option[Int]): Double = (value1, value2) match {
    case(None, _) => 0.0
    case(_, None) => 0.0
    case(Some(value1),Some(value2)) => math.pow(value1-value2,2)

  }
  def totalMeanSquareGradeDifference(currentUserEvaluations: List[Evaluation], otherUserEvaluations: List[Evaluation]): Double ={
    val currentUserSortedEvaluations = currentUserEvaluations.sortBy(_.movieId).map(_.grade)
    val otherUserSortedEvaluations = otherUserEvaluations.sortBy(_.movieId).map(_.grade)
    (currentUserSortedEvaluations zip otherUserSortedEvaluations).map{ case(currentUserGrade, otherUserGrade) =>
      calculateOptionPartialError(currentUserGrade, otherUserGrade)
    }.sum / currentUserSortedEvaluations.length

  }

  def findSimilarUsers(usersEvaluations: List[Evaluation], otherUsers: Map[Int, List[Evaluation]], similarUserCount: Int): List[Int] ={
    val distances = otherUsers.map{ case(user, evaluations) =>
      (user, totalMeanSquareGradeDifference(usersEvaluations, evaluations))
    }.toList
    distances.sortBy(_._2).map(_._1).take(similarUserCount)
  }

  def findEvaluation(evalId: Int, userId: Int, movieId: Int, userEvaluations: Map[Int, List[Evaluation]], similarusers: List[Int]): (Int, Int, Int, Int) ={
    val allSimilarUsersEvaluations = similarusers.flatMap(user => userEvaluations(user))
    println(s"Calculating similar evaluations for ${userId} allSimilarEvaluations.count ${allSimilarUsersEvaluations.size}")
    val givenMovieEvaluations = allSimilarUsersEvaluations.filter( evaluation => evaluation.movieId == movieId )
    val averageGrade = givenMovieEvaluations.map(_.grade).flatten
    (evalId, userId, movieId, (averageGrade.sum / averageGrade.length))
  }

  def predictionLine(touple: (Int, Int, Int, Int)): String ={
    predictionLine(touple._1, touple._2, touple._3, touple._4)
  }
  def predictionLine(evalId:Int, userId:Int, movieId: Int, evaluation: Int): String ={
    "%d;%d;%d;%d".format(evalId, userId, movieId, evaluation)
  }

  override def main(args: Array[String]): Unit = {
    val movies = readMovies(args(0))
    val usersEvaluation = readTrainSetFile(args(1), movies)
    usersEvaluation.foreach{ e=> require(e._2.size == movies.size, "Not enough evals")}
    val similarUsersCount = 100
    val userWithSimilarUsersTouples = usersEvaluation.par.map{ case(user, evaluations) =>
      println(s"Finding similar evaluations for ${user}")
      (user,findSimilarUsers(evaluations, usersEvaluation - user, similarUsersCount))
    }.seq

    val unkows = readUnknowns(args(2))
    val resultLines = unkows.par.map{ case(evalId, userId, movieId) =>
      predictionLine(findEvaluation(evalId, userId, movieId, usersEvaluation, userWithSimilarUsersTouples(userId)))
    }

    val pw = new PrintWriter(new File(args(3)))
    resultLines.foreach{ line =>
      pw.write(line)
      pw.write("\n")
    }
    pw.close()

  }
}