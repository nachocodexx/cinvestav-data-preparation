package mx.cinvestav
import cats.effect.{ExitCode, IO, IOApp}
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{ExchangeName, QueueName, RoutingKey}
import mx.cinvestav.Declarations.{NodeContext, NodeState, Task}
import mx.cinvestav.commons.balancer.LoadBalancer
import mx.cinvestav.config.DefaultConfig
import mx.cinvestav.server.HttpServer
import mx.cinvestav.utils.RabbitMQUtils
import mx.cinvestav.utils.v2.{Acker, Exchange, MessageQueue, PublisherConfig, PublisherV2, RabbitMQContext}
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig._
import pureconfig.generic.auto._

import java.net.InetAddress

object Main extends IOApp{
  implicit val config:DefaultConfig = ConfigSource.default.loadOrThrow[DefaultConfig]
  implicit val rabbitMQConfig: Fs2RabbitConfig = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]

  def mainProgram(queueName: QueueName)(implicit ctx:NodeContext): IO[Unit] = {
    val client     = ctx.rabbitContext.client
    val connection = ctx.rabbitContext.connection
     client.createChannel(conn = connection).use{ implicit channel=>
      for {
        (_acker,consumer) <- client.createAckerConsumer(queueName = queueName)
        _ <- consumer.evalMap{ implicit envelope =>
          val maybeCommandId = envelope.properties.headers.get("commandId")
          implicit val acker: Acker = Acker(_acker)
          maybeCommandId match {
            case Some(commandId) => commandId match {
              case StringVal(value)  if (value == "SLICE_COMPRESS")=> CommandHandlers.sliceAndCompress()
              case StringVal(value)  if (value == "SLICE")=> CommandHandlers.slice()
              case StringVal(value)  if (value == "COMPRESS")=> CommandHandlers.compress()
              case StringVal(value)  if (value == "COMPRESS_COMPLETED")=> CommandHandlers.compressCompleted()
//            _________________________________________________________________________________________________
              case StringVal(value) if (value =="MERGE_DECOMPRESS") => CommandHandlers.mergeAndDecompress()
              case StringVal(value)  if (value == "MERGE")=> CommandHandlers.merge()
//              case StringVal(value)  if (value == "MERGE_COMPLETED")=> CommandHandlers.mergeCompleted()
              case StringVal(value)  if (value == "DECOMPRESS")=> CommandHandlers.decompress()
              case StringVal(value)  if (value == "DECOMPRESS_COMPLETED")=> CommandHandlers.decompressCompleted()
              //        _ <- L.debug(s"MERGE_COMPRESS ${payload.sourcePath}")
              case StringVal(value)   => ctx.logger.error(s"COMMAND_ID[$value] NOT MATCH") *> acker.reject(envelope.deliveryTag)
            }
            case None => for {
              _ <- acker.reject(envelope.deliveryTag)
              _ <- ctx.logger.debug("NO COMMAND_ID PROVIDED")
            } yield ()
          }
        }.compile.drain
      }  yield ( )
    }
  }
  //
  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.initV2[IO](rabbitMQConfig) { implicit client =>
      client.createConnection.use{ implicit connection=>
        for {
          _               <- Logger[IO].debug(s"DATA_PREPARATION[${config.nodeId}]")
          implicit0(rabbitContext:RabbitMQContext)   <- IO.pure(RabbitMQContext(client=client,connection=connection))
          poolId          = config.poolId
          nodeId          = config.nodeId
//        ____________________________________________________________________________
          exchangeName  = ExchangeName(s"$poolId-data_preparation")
          queueName     = QueueName(s"$poolId-$nodeId")
          routingKey    = RoutingKey(s"$poolId.$nodeId")
//         ______________________________________________________________-
          exchange      <- Exchange.topic(exchangeName = exchangeName)
          queue         <- MessageQueue.createThenBind(
            queueName = queueName,
            exchangeName = exchangeName,
            routingKey = routingKey
          )
          //         __________________________________________________________
          publishers      = config.dataPreparationNodes.map{ node=>
            val routingKey = RoutingKey(s"${config.poolId}.${node.nodeId}")
            (node.nodeId,PublisherConfig(exchangeName = exchangeName,routingKey = routingKey))
          }
            .map(x=>x.copy(_2 = PublisherV2.create(x._1,x._2)))
            .toMap
          initState       = NodeState(
            sourceFolders = config.sourceVolumes,
            publishers    = publishers,
            loadBalancer  = LoadBalancer("RB"),
            pendingTasks = Map.empty[String,Task],
            ip = InetAddress.getLocalHost.getHostAddress,
          )
          state           <- IO.ref(initState)
          context         = NodeContext(state=state,rabbitContext = rabbitContext,logger = unsafeLogger,config=config)
          _             <- mainProgram(queueName = queueName)(ctx=context).start
          _             <- HttpServer.run()(ctx=context)
        } yield ()
      }
    }.as(ExitCode.Success)
  }
}
