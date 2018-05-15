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

package com.art4ul.pq.sql

import com.art4ul.antlr.{SqlGrammarLexer, SqlGrammarParser}
import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.antlr.v4.runtime.{ANTLRInputStream, CommonTokenStream}

trait Ast

trait Projection {
  val columns: Seq[Column]
}

case object EmptyProjection extends Projection {
  override val columns: Seq[Column] = Seq()
}

case object AllColumnProjection extends Projection {
  override val columns: Seq[Column] = Seq()
}

case class ColumnProjection(override val columns: Seq[Column]) extends Projection

case class Column(val name: String) extends Ast

case class Or(left: Ast, right: Ast) extends Ast

case class And(left: Ast, right: Ast) extends Ast

case class Not(left: Ast) extends Ast

case class ColumnValue(name: String, value: String) extends Ast

case class Eq(value: Ast) extends Ast

case class NotEq(value: Ast) extends Ast

case class Gt(value: Ast) extends Ast

case class GtEq(value: Ast) extends Ast

case class Lt(value: Ast) extends Ast

case class LtEq(value: Ast) extends Ast

case object EmptyAst extends Ast

case class SqlQuery(projection: Projection = EmptyProjection, filter: Ast = EmptyAst)

object SqlParser {

  def parse(query: String): SqlQuery = {
    val lexer = new SqlGrammarLexer(new ANTLRInputStream(query))
    lexer.removeErrorListeners()
    val tokens = new CommonTokenStream(lexer)
    val parser = new SqlGrammarParser(tokens)
    parser.removeErrorListeners()
    //    parser.setErrorHandler(new )
    val listener = new SqlRuleListener
    val walker = new ParseTreeWalker
    walker.walk(listener, parser.sqlRule)
    listener.getResultQuery
  }

}
