package com.art4ul.pq

import java.util.logging.LogManager

import ExecutionContext._
import com.art4ul.pq.outputformat.OutputFormatter
import com.art4ul.pq.parquet.{MetadataManager, ParquetIO}
import org.apache.hadoop.conf.Configuration

object Main {



  def main(args: Array[String]): Unit = {
    LogManager.getLogManager().readConfiguration(getClass.getResourceAsStream("/logging.properties"));
    withConfig(args) { implicit ctx =>
      implicit val fsConfig = new Configuration()
      val io = new ParquetIO(fsConfig)
      val schema = MetadataManager.commonSchema(ctx.paths)
      val records = io.readParquets(ctx.paths: _*)()

      val formatter = OutputFormatter(schema)
      formatter.format(records)
    }

  }

}
