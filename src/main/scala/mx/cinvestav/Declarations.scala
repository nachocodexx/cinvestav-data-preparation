package mx.cinvestav

import cats.effect.{IO, Ref}
//import mx.cinvestav.Declarations.payloads.CompressedChunkLocation
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.utils.v2.{PublisherV2, RabbitMQContext}
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons.balancer.LoadBalancer
import mx.cinvestav.commons.fileX.{Extension, Filename}
import mx.cinvestav.commons.types._

object Declarations {
  case class NodeState(
                        publishers:Map[String,PublisherV2],
                        loadBalancer:LoadBalancer,
                        loadBalancerPublisher:PublisherV2,
                        pendingTasks:Map[String,Task],
                        ip:String
                      )
  case class NodeContext(
                          state:Ref[IO,NodeState],
                          rabbitContext: RabbitMQContext,
                          logger:Logger[IO],
                          config:DefaultConfig
                        )


}
