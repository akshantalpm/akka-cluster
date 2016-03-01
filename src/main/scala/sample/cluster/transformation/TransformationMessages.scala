package sample.cluster.transformation

final case class TransformationJob(text: String)
final case class TransformationResult(text: String)
final case class JobFailed(reason: String, job: TransformationJob)
case object TransformersRegistration
case object PimRegistration
case object CadcRegistration