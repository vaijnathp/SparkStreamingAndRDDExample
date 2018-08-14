package polsys.scalaExamples

/**
  * Created by vaijnathp on 6/19/2018.
  */
object Variance {
  def main(args: Array[String]): Unit = {
    val animals:List[Animal] =List(new Animal("horse"),new Animal("dog"))
    val cats:List[Cat]=List(new Cat("brown","catiee"),new Cat("White","Mawwww"))

    val animalAsCatList:List[Animal]=cats
  }
}

class Animal(name: String) {
  println("Name: " + name)
}

class Cat(color: String, name: String) extends Animal(name) {
  println("Color:" + color)
}

