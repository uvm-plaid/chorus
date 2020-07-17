package chorus.mechanisms

import chorus.analysis.differential_privacy.GlobalSensitivityAnalysis
import chorus.schema.Database
import chorus.rewriting.RewriterConfig
import chorus.sql.relational_algebra.Relation
import chorus.rewriting.differential_privacy.ClippingRewriter
import chorus.util.DB

class LaplaceMechClipping(epsilon: Double, l: Double, u: Double,
  root: Relation, config: RewriterConfig)
    extends ChorusMechanism[List[DB.Row]] {

  def getSensitivities(query: Relation, database: Database): Seq[Double] = {
    val a = new GlobalSensitivityAnalysis()
    val facts = a.run(query, database).colFacts
    facts.map(_.sensitivity.get)
  }

  def run() = {
    val clippedQuery = new ClippingRewriter(config, l, u).run(root).root

    val sensitivities = getSensitivities(clippedQuery, config.database)
    val scales = sensitivities.map(_ / epsilon)

    val result = DB.execute(clippedQuery, config.database)
    (BasicMechanisms.laplace(result, scales), EpsilonDPCost(epsilon))
  }

}

