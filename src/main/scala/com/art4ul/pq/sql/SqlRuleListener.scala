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

import com.art4ul.antlr.{SqlGrammarBaseListener, SqlGrammarParser}

import scala.collection.mutable
import scala.collection.JavaConversions._

class SqlRuleListener extends SqlGrammarBaseListener {

  private val stack = new mutable.Stack[Ast]()
  private var query = SqlQuery()

  def getResultQuery: SqlQuery = query

  private def nonEmpty(value: Any): Boolean = {
    value != null
  }

  override def exitSqlRule(ctx: SqlGrammarParser.SqlRuleContext): Unit = {
    if (stack.nonEmpty){
      query = query.copy(filter = stack.pop())
    }
  }

  override def exitProjection(ctx: SqlGrammarParser.ProjectionContext): Unit = {
    val projection = ctx.ID.map(_.getText.toLowerCase).toList match {
      case Nil if (ctx.ALL().getText != null) => AllColumnProjection
      case Nil => EmptyProjection
      case other => ColumnProjection(other.map(c => Column(c)))
    }
    query = query.copy(projection = projection)
  }




  override def exitFilterPredicate(ctx: SqlGrammarParser.FilterPredicateContext): Unit = {
    val result = ctx match {
      case c if (nonEmpty(c.AND())) =>
        val (second,first) = (stack.pop(), stack.pop())
        Some(And(first, second))
      case c if (nonEmpty(c.OR())) =>
        val (second,first) = (stack.pop(), stack.pop())
        Some(Or(first, second))
      case c if (nonEmpty(c.NOT())) => Some(Not(stack.pop()))
      case _ => None
    }
    result.foreach(stack.push)
  }


  override def exitValuePredicate(ctx: SqlGrammarParser.ValuePredicateContext): Unit = {
    val result: Ast = ctx match {
      case c if (nonEmpty(c.EQ())) => Eq(ColumnValue(ctx.ID().getText.toLowerCase,ctx.value().getText))
      case c if (nonEmpty(c.NOT_EQ())) => NotEq(ColumnValue(ctx.ID().getText.toLowerCase,ctx.value().getText))
      case c if (nonEmpty(c.GT())) => Gt(ColumnValue(ctx.ID().getText.toLowerCase,ctx.value().getText))
      case c if (nonEmpty(c.LT())) => Lt(ColumnValue(ctx.ID().getText.toLowerCase,ctx.value().getText))
      case c if (nonEmpty(c.GT_EQ())) => GtEq(ColumnValue(ctx.ID().getText.toLowerCase,ctx.value().getText))
      case c if (nonEmpty(c.LT_EQ())) => LtEq(ColumnValue(ctx.ID().getText.toLowerCase,ctx.value().getText))
      case _ => throw new UnsupportedOperationException()
    }
    stack.push(result)
  }


}
