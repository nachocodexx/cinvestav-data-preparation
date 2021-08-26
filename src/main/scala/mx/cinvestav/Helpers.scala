package mx.cinvestav

import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.types.ChunkMetadata
//import mx.cinvestav.Declarations.payloads.CompressedChunkLocation
import mx.cinvestav.commons.payloads.v2.CompressedChunkLocation
import mx.cinvestav.commons.fileX.FileMetadata
import mx.cinvestav.server.Client.{downloadFile, downloadFileE}

import java.nio.file.{Path, Paths}

object Helpers {

  def downloadUncompressedChunks(
                                compressedChunkLocation: List[ChunkMetadata],
                                destination:Path
                              ): IO[List[FileMetadata]] = {
    compressedChunkLocation.traverse{ chunkMetadata=>
      val url     = chunkMetadata.location.url
      val sources = chunkMetadata.location.source::Nil
      sources.traverse{ source =>
        val metadata = FileMetadata.fromPath(Paths.get(source))
        val chunkName = metadata.fullname
        val chunkDestination = destination.resolve(chunkName)
        downloadFile(url,source,destination= chunkDestination)
          .handleError{ e=>
            IO.println(s"ERROR: ${e.getMessage}")
          }.map(_=>metadata)
      }
      //        .flatMap(_=>IO.unit)
    }.map(_.flatten)
    //      .flatMap(_=>IO.unit)
    //      .flatMap(_.flatten)

  }


  def downloadCompressedChunks(
                                compressedChunkLocation: List[CompressedChunkLocation],
                                destination:Path
                              ): IO[List[FileMetadata]] = {
    compressedChunkLocation.traverse{ chunksLocation=>
      val url     = chunksLocation.url
      val sources = chunksLocation.sources
      sources.traverse{ source =>
        val metadata = FileMetadata.fromPath(Paths.get(source))
        val chunkName = metadata.fullname
        val chunkDestination = destination.resolve(chunkName)
        downloadFile(url,source,destination= chunkDestination)
          .handleError{ e=>
            IO.println(s"ERROR: ${e.getMessage}")
          }.map(_=>metadata)
      }
//        .flatMap(_=>IO.unit)
    }.map(_.flatten)
//      .flatMap(_=>IO.unit)
//      .flatMap(_.flatten)

  }


}
