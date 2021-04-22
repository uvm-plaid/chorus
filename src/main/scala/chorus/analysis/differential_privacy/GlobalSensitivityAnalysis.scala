package chorus.analysis.differential_privacy

import chorus.analysis.histogram.{HistogramAnalysis, QueryType}
import chorus.dataflow.AggFunctions._
import chorus.dataflow.column.{NodeColumnFacts, RelNodeColumnAnalysis}
import chorus.exception.{UnsupportedConstructException, UnsupportedQueryException}
import chorus.schema.{Database, Schema}
import chorus.sql.relational_algebra._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Aggregate, Join, TableScan}
import org.apache.calcite.rex.{RexCall, RexNode, RexLiteral}
import org.apache.calcite.sql.SqlKind
import collection.JavaConverters._

/** Global sensitivity analysis
  */
class GlobalSensitivityAnalysis extends RelNodeColumnAnalysis(StabilityDomain, SensitivityDomain) {

  val checkBinsForRelease: Boolean =
    System.getProperty("dp.elastic_sensitivity.check_bins_for_release", "true").toBoolean

  override def run(_root: RelOrExpr, database: Database):
      NodeColumnFacts[RelStability,ColSensitivity] = {
    val root = _root.unwrap.asInstanceOf[RelNode]

    super.run(root, database)
  }


  override def transferAggregate(node: Aggregate,
    aggFunctions: IndexedSeq[Option[AggFunction]],
    state: NodeColumnFacts[RelStability,ColSensitivity]):
      NodeColumnFacts[RelStability,ColSensitivity] = {
    val numGroupedCols = aggFunctions.count{ _.isEmpty }
    val newNodeFact =
      // Histograms introduce a factor of 2 to the stability of the relation
      if (numGroupedCols > 0)
        state.nodeFact.copy(stability = 2 * state.nodeFact.stability)
      else
        state.nodeFact

    val newColFacts = state.colFacts.zipWithIndex.map { case (colState, idx) =>
      val aggFunction = aggFunctions(idx)
      aggFunction match {
        case None => // histogram bin
          if (checkBinsForRelease && !colState.canRelease) {
            val binName = node.getRowType.getFieldNames.get(idx)
            throw new ProtectedBinException(binName)
          }

          // Sensitivity of bins that are safe for release is zero (i.e. no noise added)
          colState.copy(sensitivity = Some(0.0))

        case Some(func) => // aggregated column
          val newSensitivity = func match {
            case COUNT => newNodeFact.stability
            case SUM   =>
              (colState.lowerBound, colState.upperBound) match {
                case (Some(l), Some(u)) => newNodeFact.stability * (u - l)
                case _ => Double.PositiveInfinity
              }
            case _     => Double.PositiveInfinity
          }

          colState.copy(
            sensitivity = Some(newSensitivity),
            maxFreq = Double.PositiveInfinity,
            aggregationApplied = true,
            postAggregationArithmeticApplied = colState.aggregationApplied,
            canRelease = false,
            upperBound = None,
            lowerBound = None)
      }
    }

    NodeColumnFacts(newNodeFact, newColFacts)
  }

  override def transferTableScan(node: TableScan,
    state: NodeColumnFacts[RelStability,ColSensitivity]):
      NodeColumnFacts[RelStability,ColSensitivity] = {
    // Fetch metadata for the table
    val tableName = RelUtils.getQualifiedTableName(node)
    val isTablePublic = Schema.getTableProperties(this.getDatabase, tableName).get("isPublic").fold(false)(_.toBoolean)

    val newColFacts = state.colFacts.zipWithIndex.map { case (colState, idx) =>
      // Fetch metadata for this column
      val colName = node.getRowType.getFieldNames.get(idx)
      val colProperties = Schema.getSchemaMapForTable(this.getDatabase, tableName)(colName).properties

      val canRelease = colProperties.get("canRelease").fold(false)(_.toBoolean) || isTablePublic

      colState.copy(
        canRelease = canRelease
      )
    }

    val newNodeFact = state.nodeFact.copy(
      stability = 1.0,
      ancestors = Set(tableName)
    )

    NodeColumnFacts(newNodeFact, newColFacts)
  }

  override def transferExpression(node: RexNode, state: ColSensitivity): ColSensitivity = {
    node match {
      case c: RexCall =>
        val isAllowedExpression =
          List(
            SqlKind.CASE,
            SqlKind.EQUALS,
            SqlKind.CAST,
            SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL,
            SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL
          )

        if (!isAllowedExpression.contains(c.getOperator.getKind))
          throw new UnsupportedConstructException("Unsupported operator: " + c)


        c.getOperator.getKind match {
          case SqlKind.CASE => {
            val operands = c.getOperands.asScala
            val comparison = operands(0)
            val bound = operands(1).asInstanceOf[RexLiteral].getValue
              .asInstanceOf[java.math.BigDecimal].doubleValue

            comparison match {
              case cmp: RexCall => {

                // TODO: this needs some error checking
                cmp.getOperator.getKind match {
                  case SqlKind.GREATER_THAN => 
                    state.copy(
                      sensitivity = Some(Double.PositiveInfinity),
                      maxFreq = Double.PositiveInfinity,
                      upperBound = Some(bound))
                  case SqlKind.LESS_THAN =>
                    state.copy(
                      sensitivity = Some(Double.PositiveInfinity),
                      maxFreq = Double.PositiveInfinity,
                      lowerBound = Some(bound))
                  case _ =>
                    state.copy(
                      sensitivity = Some(Double.PositiveInfinity),
                      maxFreq = Double.PositiveInfinity,
                      lowerBound = None)
                }
              }
            }
          }

          case _ => {
            val isArithmeticExpression =
              !List(SqlKind.EQUALS, SqlKind.CAST).contains(c.getOperator.getKind)
            state.copy(
              sensitivity = Some(Double.PositiveInfinity),
              maxFreq = Double.PositiveInfinity,
              postAggregationArithmeticApplied = state.aggregationApplied && isArithmeticExpression)

          }
        }

      case _ =>
        // conservatively handle all other expression nodes, which
        // could arbitrarily alter column values such that current
        // metrics are invalidated (e.g., in the case of arithmetic
        // expressions).
        state.copy(maxFreq = Double.PositiveInfinity)
    }
  }

}

