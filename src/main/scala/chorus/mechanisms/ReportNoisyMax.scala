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


class ReportNoisyMax(epsilon: Double, queries: List[Relation], config: RewriterConfig)
    extends ChorusMechanism[Int] {

  def run() = {
    val results = queries.map { (q: Relation) =>
      new LaplaceMechClipping(epsilon, 0, 1, q, config).run()._1 }
    val unwrappedResults : List[Double] =
      results.map { case List(DB.Row(List(i))) => i.toDouble }

    (BasicMechanisms.argmax(unwrappedResults), EpsilonDPCost(epsilon))
  }
}

