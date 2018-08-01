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
import com.art4ul.pq.action.ContentViewer.{CatAction, HeadAction, TailAction}
import com.art4ul.pq.action.{SchemaAction, UndefinedAction}
import org.slf4j.LoggerFactory

object Main {

  val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    LogManager.getLogManager().readConfiguration(getClass.getResourceAsStream("/logging.properties"));
    withConfig(args) { implicit ctx =>
      val printer = System.out
      val action = ctx.cmd match {
        case CmdType.Cat => new CatAction(printer)
        case CmdType.Head => new HeadAction(printer)
        case CmdType.Tail => new TailAction(printer)
        case CmdType.Schema => new SchemaAction(printer)
        case _ => UndefinedAction
      }

      action.action()

    }

  }

}
