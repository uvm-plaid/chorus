package chorus.mechanisms

abstract class ChorusMechanism[A] {
  def run(): (A, PrivacyCost)

  def execute(accountant: PrivacyAccountant): A = {
    val (result, cost) = run()
    accountant.addCost(cost)
    result
  }
}
