package mx.cinvestav.config

case class DataPreparationNode(nodeId:String,index:Int)
case class LoadBalancerInfo(exchange:String, routingKey:String)
case class DefaultConfig(
                          nodeId:String,
                          poolId:String,
                          host:String,
                          port:Int,
                          loadBalancer:LoadBalancerInfo,
                          exchangeName:String,
                          //                          sourceVolumes:List[String],
                          //                          sinkVolumes:List[String],
                          sinkFolder:String,
                          dataPreparationNodes:List[DataPreparationNode],
                          rabbitmq: RabbitMQClusterConfig
                        )
