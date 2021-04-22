package chorus

import chorus.rewriting.differential_privacy.{ElasticSensitivityConfig, ElasticSensitivityRewriter}
import chorus.schema.Schema
import chorus.sql.QueryParser
import junit.framework.TestCase
import chorus.analysis.differential_privacy.GlobalSensitivityAnalysis
import chorus.rewriting.RewriterConfig
import chorus.sql.relational_algebra.Relation
import chorus.rewriting.differential_privacy.ClippingRewriter
import chorus.exception.{UnsupportedConstructException, UnsupportedQueryException}

class ChorusTests extends TestCase {
  val database = Schema.getDatabase("test")
  val config = new RewriterConfig(database)

  def checkResult(query: String, expected: Seq[Double]): Unit = {
    val root = QueryParser.parseToRelTree(query, database)
    val clippedQuery = new ClippingRewriter(config, 0, 10).run(root).root

    val sensitivities = getSensitivities(clippedQuery)
    TestCase.assertEquals(sensitivities, expected)
  }

  def getSensitivities(query: Relation): Seq[Double] = {
    val a = new GlobalSensitivityAnalysis()
    val facts = a.run(query, database).colFacts
    facts.map(_.sensitivity.get)
  }


  def testMathExpressions() = {
    val query1 = "SELECT SUM(order_cost) FROM orders WHERE product_id = 1"
    checkResult(query1, List(10.0))

    val query2 = "SELECT SUM(order_cost/0) FROM orders WHERE product_id = 1"
    try {
      checkResult(query2, List(0))
    } catch {
      case e: UnsupportedConstructException => TestCase.assertTrue(true)
    }

  }
}
