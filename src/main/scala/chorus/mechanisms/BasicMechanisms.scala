package chorus.mechanisms

import chorus.util.DB

object BasicMechanisms {

  def laplaceSample(scale: Double): Double = {
    val u = 0.5 - scala.util.Random.nextDouble()
    -math.signum(u) * scale * math.log(1 - 2*math.abs(u))
  }

  def laplace(vals: List[DB.Row], scales: Seq[Double]): List[DB.Row] =
    vals.map { (row: DB.Row) =>
      DB.Row((row.vals zip scales).map {
        case (v, scale) =>
          (v.toDouble + laplaceSample(scale)).toString
      })
    }

  def argmax(vals: List[Double]): Int =
    vals.view.zipWithIndex.maxBy(_._1)._2

  def chooseWithProbability[A](probabilities: List[(A, Double)]): Int =
    0 // probabilities(0)
}
