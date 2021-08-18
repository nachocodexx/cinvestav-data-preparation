package mx.cinvestav.config

case class DataPreparationNode(nodeId:String,index:Int)
case class DefaultConfig(
                          nodeId:String,
                          poolId:String,
                          host:String,
                          port:Int,
                          loadBalancer:String,
                          sourceVolumes:List[String],
                          sinkVolumes:List[String],
                          sinkFolder:String,
                          dataPreparationNodes:List[DataPreparationNode],
                          rabbitmq: RabbitMQClusterConfig
                        )
