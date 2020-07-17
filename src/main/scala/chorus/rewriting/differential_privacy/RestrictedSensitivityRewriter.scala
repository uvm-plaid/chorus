package chorus.rewriting.differential_privacy

import chorus.analysis.differential_privacy.RestrictedSensitivityAnalysis
import chorus.rewriting.DPRewriterConfig
import chorus.schema.Database
import chorus.sql.relational_algebra.Relation

/** Rewriter that enforces differential privacy using Restricted Sensitivity. */
class RestrictedSensitivityRewriter(config: RestrictedSensitivityConfig) extends SensitivityRewriter(config) {
  def getLaplaceNoiseScale(node: Relation, colIdx: Int): Double =
    new RestrictedSensitivityAnalysis().run(node, config.database).colFacts(colIdx).sensitivity.get / config.epsilon
}

class RestrictedSensitivityConfig(
    override val epsilon: Double,
    override val database: Database,
    override val fillMissingBins: Boolean = true)
  extends DPRewriterConfig(epsilon, database, fillMissingBins)
