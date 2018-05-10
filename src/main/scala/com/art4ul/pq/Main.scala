/*
 * Copyright (C) 2018 Artsem Semianenka (http://art4ul.com/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
      def records = io.readParquets(ctx.paths: _*)()

      val formatter = OutputFormatter(schema)
      try {
        formatter.format(records)
      }finally {
        formatter.close()
      }
    }

  }

}
