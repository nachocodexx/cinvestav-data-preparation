import mx.cinvestav.commons.balancer.LoadBalancer
import mx.cinvestav.server.Client

import java.nio.file.Paths
import java.util.UUID

class DataPreparationSpec extends munit .CatsEffectSuite {
  test("Basics"){
    val chunkName = UUID.randomUUID().toString
    val data      = chunkName.split('.').toList.lastOption
    println(data )
  }
  test("Download file"){
    Client.downloadFile(
      "http://localhost:7001/download",
      "/home/nacho/Programming/Scala/data-preparation/target/sources/01.pdf",
      Paths.get("/home/nacho/Programming/Scala/data-preparation/target/sink/data/dp-0/FILE_ID/compressed/01.pdf")
    )
  }
  test("Balancer"){
    val lb = LoadBalancer("RB")
    val nodeIds = List("dp-0","dp-1","dp-2")
    val numOfChunks = 23
    val balanced = lb.balanceMulti(nodeIds,numOfChunks,allowRep=true)
    println(balanced)
  }

}
