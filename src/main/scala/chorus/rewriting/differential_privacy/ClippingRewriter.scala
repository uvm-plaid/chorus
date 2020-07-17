package chorus.rewriting.differential_privacy

import chorus.rewriting.rules.ValueExpr
import chorus.rewriting.{RewriterConfig, DPRewriterConfig, DPUtil, Rewriter}
import chorus.exception.UnsupportedQueryException
import chorus.sql.relational_algebra.{RelUtils, Relation}

import chorus.rewriting.rules.ColumnDefinition._
import chorus.rewriting.rules.Operations._
import chorus.rewriting.rules.ValueExpr
import chorus.rewriting.rules.Expr._

import chorus.dataflow.domain.UnitDomain

import org.apache.calcite.rel.core.Aggregate
import org.apache.calcite.sql.fun.SqlSumAggFunction

class ClippingRewriter[C <: RewriterConfig](config: C, l: Double, u: Double)
    extends Rewriter(config) {
  val lowerBound = l
  val upperBound = u

  def clamp(expr: ValueExpr, min: ValueExpr, max: ValueExpr): ValueExpr =
    Case(expr < min, min, Case(expr > max, max, expr))

  def rewrite(root: Relation): Relation = {
    root.rewriteRecursive(UnitDomain) { (node, orig, _) =>

      node match {
        case Relation(a: Aggregate) => {
          val groupedCols = RelUtils.getGroupedCols(a).map {c => c.idx}

          val newR = Relation(a.getInput).mapCols { colDef =>
            if (groupedCols contains colDef.idx)
              col(colDef) AS colDef.alias
            else
              clamp(col(colDef), lowerBound, upperBound) AS colDef.alias
          }

          val finalResult = Relation(a).replaceInputs { rs =>
            if (rs.length > 1)
              throw new UnsupportedQueryException("This rewriter only works on single-table queries")
            List(newR)
          }

          (finalResult, ())
        }
        case _ => (node, ())
      }
    }
  }
}
