package sample.cluster.transformation

object TransformationApp {

  def main(args: Array[String]): Unit = {
    Pim.main(Seq("2551").toArray)
    Cadc.main(Array.empty)
    Cadc.main(Array.empty)
    Transformers.main(Seq("2552").toArray)
    Transformers.main(Seq.empty.toArray)
  }

}