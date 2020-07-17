package chorus.mechanisms

import scala.collection.mutable.MutableList

trait PrivacyCost {
  def +(other: PrivacyCost): PrivacyCost
}

case class EpsilonDPCost(epsilon: Double) extends PrivacyCost {
  def +(other: PrivacyCost) = other match {
    case EpsilonDPCost(otherEpsilon) => EpsilonDPCost(epsilon + otherEpsilon)
  }
}

case class RenyiDPCost(alpha: Int, epsilon: Double) extends PrivacyCost {
  def +(other: PrivacyCost) = other match {
    case RenyiDPCost(otherAlpha, otherEpsilon) =>
      RenyiDPCost(math.max(alpha, otherAlpha), epsilon + otherEpsilon)
  }
}

case class EpsilonDeltaDPCost(epsilon: Double, delta: Double) extends PrivacyCost {
  def +(other: PrivacyCost) = other match {
    case EpsilonDPCost(otherEpsilon) => EpsilonDeltaDPCost(epsilon + otherEpsilon, delta)
    case EpsilonDeltaDPCost(otherEpsilon, otherDelta) =>
      EpsilonDeltaDPCost(epsilon + otherEpsilon, delta + otherDelta)
  }
}


abstract class PrivacyAccountant {
  val costs: MutableList[PrivacyCost] = MutableList()

  def getTotalCost(): PrivacyCost

  def addCost(c: PrivacyCost) = costs += c
}

class EpsilonCompositionAccountant extends PrivacyAccountant {
  def getTotalCost() = costs.fold(EpsilonDPCost(0))(_ + _)
}

class RenyiCompositionAccountant extends PrivacyAccountant {
  def getTotalCost() = costs.fold(RenyiDPCost(0, 0))(_ + _)
}


class AdvancedCompositionAccountant(delta: Double) extends PrivacyAccountant {
  def getTotalCost() = {
    val epsilons: Seq[Double] = costs.map { case EpsilonDPCost(eps) => eps }
    val totalEpsilon = 2*(epsilons.max)*math.sqrt(2*(epsilons.length)*math.log(1/delta))
    EpsilonDeltaDPCost(totalEpsilon, delta)
  }
}
