package com.longyun.scala.cep

class Event(id:Int, name: String) {


  def getId: Int = id

  def getName: String = name

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[Event]){
      val other = obj.asInstanceOf[Event]
      (this.id == other.getId
        && this.name.equals(other.getName))
    }

    false
  }

}


class SubEvent(id: Int, name: String, volume: Float) extends Event(id, name){


  def getVolume: Float = volume

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[SubEvent]){
      if(super.equals(obj)){
        val other = obj.asInstanceOf[SubEvent]
        this.volume == other.getVolume
      }
    }

    false
  }

}


class Alert(id: Int, name: String, _type: String){

}