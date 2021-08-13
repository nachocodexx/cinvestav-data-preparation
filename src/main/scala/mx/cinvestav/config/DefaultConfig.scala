package mx.cinvestav.config

case class DataPreparationNode(nodeId:String,index:Int)
case class DefaultConfig(
                          nodeId:String,
                          poolId:String,
                          sourceVolumes:List[String],
                          sinkVolumes:List[String],
                          dataPreparationNodes:List[DataPreparationNode],
                          rabbitmq: RabbitMQClusterConfig
                        )
