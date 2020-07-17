package examples

import chorus.schema.Schema
import chorus.sql.QueryParser
import chorus.mechanisms.LaplaceMechClipping
import chorus.mechanisms.AverageMechClipping
import chorus.mechanisms.EpsilonCompositionAccountant
import chorus.rewriting.RewriterConfig

object MechanismExamples extends App {
  System.setProperty("dp.elastic_sensitivity.check_bins_for_release", "false")
  System.setProperty("db.use_dummy_database", "true")

  // Use the table schemas and metadata defined by the test classes
  System.setProperty("schema.config.path", "src/test/resources/schema.yaml")
  val database = Schema.getDatabase("test")
  val config = new RewriterConfig(database)

  // Define simple test queries
  val query1 = "SELECT SUM(order_cost) FROM orders WHERE product_id = 1"
  val root1 = QueryParser.parseToRelTree(query1, database)

  val query2 = "SELECT AVG(order_cost) FROM orders"
  val root2 = QueryParser.parseToRelTree(query2, database)

  // Define the privacy accountant
  val accountant = new EpsilonCompositionAccountant()

  // Run the mechanisms
  val r1 = new LaplaceMechClipping(1.0, 0, 10, root1, config).execute(accountant)
  val r2 = new AverageMechClipping(1.0, 0, 10, root2, config).execute(accountant)

  println("Sum query result: " + r1)
  println("Average query result: " + r2)

  println("Total privacy cost: " + accountant.getTotalCost())
}
