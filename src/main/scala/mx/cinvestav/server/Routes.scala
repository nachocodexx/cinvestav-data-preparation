package mx.cinvestav.server

import cats.effect.IO
import fs2.io.file.Files
import mx.cinvestav.Declarations.NodeContext
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityDecoder._
import io.circe._
import io.circe.generic.auto._
import mx.cinvestav.commons.fileX.FileMetadata

import java.nio.file.Paths
//import io.circe.syntax._

import java.io.File

object Routes {
  case class DownloadPayload(source:String)
  def hello(implicit ctx:NodeContext): HttpRoutes[IO] = HttpRoutes.of[IO]{
    case req@GET -> Root   =>
      Ok(s"Hello I'm ${ctx.config.nodeId}")
    case req@GET ->  "v2" /: filePath => for {
      _ <- IO.unit
      path       = Paths.get(filePath.toAbsolute.toString())
      file       = path.toFile
      _          <- ctx.logger.debug(s"FILE_PATH $path")
      response   <- if(!file.exists())  NotFound()
      else for {
        _        <- ctx.logger.debug(s"FILE_SIZE ${file.length()} bytes")
        metadata = FileMetadata.fromPath(path)
        _        <- ctx.logger.debug(s"FILE_EXTENSION ${metadata.extension}")
        bytes    = Files[IO].readAll(path,4098)
        _        <- ctx.logger.info(s"DOWNLOAD ${metadata.fullname} ${metadata.size.value.get}")
        response <- Ok(bytes)
      } yield response
    } yield response
    case req@POST -> Root / "download"  => for{
      payload    <- req.as[DownloadPayload]
      source     = payload.source
      path       = Paths.get(source)
      file       = path.toFile
//      _          <- ctx.logger.debug(s"PATH $path")
//      _          <- ctx.logger.debug(s"PATH ${file.exists()}")
      response   <- if(!file.exists())  NotFound()
      else for {
        _        <- ctx.logger.debug(s"FILE_PATH $path")
        _        <- ctx.logger.debug(s"FILE_SIZE ${file.length()} bytes")
        metadata = FileMetadata.fromPath(path)
        _        <- ctx.logger.debug(s"FILE_EXTENSION ${metadata.extension}")
        bytes    = Files[IO].readAll(path,4098)
        _        <- ctx.logger.info(s"DOWNLOAD ${metadata.fullname} ${metadata.size.value.get}")
        response <- Ok(bytes)
      } yield response
    } yield response
  }

  def apply()(implicit ctx:NodeContext): HttpRoutes[IO] = hello

}
