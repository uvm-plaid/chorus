package chorus.mechanisms


import chorus.analysis.differential_privacy.GlobalSensitivityAnalysis
import chorus.schema.Database
import chorus.rewriting.RewriterConfig
import chorus.rewriting.differential_privacy.ClippingRewriter
import chorus.util.DB
import chorus.sql.relational_algebra.{RelUtils, Relation}
import chorus.dataflow.domain.UnitDomain
import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.sql.fun.SqlSumAggFunction
import chorus.exception.UnsupportedQueryException

import chorus.rewriting.rules.ColumnDefinition._
import chorus.rewriting.rules.Operations._
import chorus.rewriting.rules.ValueExpr
import chorus.rewriting.rules.Expr._


class ExponentialMechanism(epsilon: Double, scoring: Relation, config: RewriterConfig)
    extends ChorusMechanism[Int] {

  def run() = {
    val sensitivity = new GlobalSensitivityAnalysis().run(scoring, config.database)
      .colFacts.map(_.sensitivity.get).max

    val scores = DB.execute(scoring, config.database)
    val totalScore = scores.map { case DB.Row(List(_, v)) => v.toDouble }.sum

    val probabilities = scores.map { case DB.Row(List(k, v)) =>
      (k, epsilon * (v.toDouble / totalScore) / (2 * sensitivity)) }

    (BasicMechanisms.chooseWithProbability(probabilities),
      EpsilonDPCost(epsilon))
  }
}
