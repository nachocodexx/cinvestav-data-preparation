import mx.cinvestav.commons.balancer.LoadBalancer

class DataPreparationSpec extends munit .CatsEffectSuite {
  test("Balancer"){
    val lb = LoadBalancer("RB")
    val nodeIds = List("dp-0","dp-1","dp-2")
    val numOfChunks = 23
    val balanced = lb.balanceMulti(nodeIds,numOfChunks,allowRep=true)
    println(balanced)
  }

}
