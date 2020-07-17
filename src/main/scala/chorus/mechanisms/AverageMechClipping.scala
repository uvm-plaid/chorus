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

class AverageMechClipping(epsilon: Double, l: Double, u: Double,
  root: Relation, config: RewriterConfig)
    extends ChorusMechanism[List[DB.Row]] {

  def replaceAgg(root: Relation, aggFn: String) = {
    root.rewriteRecursive(UnitDomain) { (node, orig, _) =>
      node match {
        case Relation(a: Aggregate) => {
          val groupedCols = RelUtils.getGroupedCols(a)

          val origAlias = a.getRowType.getFieldNames.get(groupedCols.length)
          val origCol = a.getInput().getRowType.getFieldNames.get(groupedCols.length)

          val finalAgg = aggFn match {
            case "sum" => Sum(col(origCol)) AS origAlias
            case "count" => Count(col(origCol)) AS origAlias
          }

          val newR = Relation(a.getInput).agg(groupedCols: _*)(finalAgg)

          (newR, ())
        }
        case _ => (node, ())
      }
    }
  }

  def run() = {
    val sumQuery = replaceAgg(root, "sum")
    val countQuery = replaceAgg(root, "count")

    val (r1, c1) = new LaplaceMechClipping(epsilon/2.0, 0, 10, sumQuery, config).run()
    val (r2, c2) = new LaplaceMechClipping(epsilon/2.0, 0, 10, countQuery, config).run()

    val results = (r1 zip r2).map {
      case (DB.Row(vs1), DB.Row(vs2)) => DB.Row((vs1 zip vs2).map {
        case (n1, n2) => (n1.toDouble / n2.toDouble).toString
      })
    }

    (results, c1 + c2)
  }
}
