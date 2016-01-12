package tul.poiis.similar_users_evaluator

import tul.poiis.similar_users_evaluator.Evaluation.MovieId

/**
  * Created by michal on 11.01.2016.
  */
object Evaluation{
  type MovieId = Int
}
case class Evaluation(val movieId: MovieId, val grade: Option[Int]) {

}
