package com.art4ul.pq.action

trait Action {

  def action(): Unit

}

object UndefinedAction extends Action{
  override def action(): Unit = {}
}