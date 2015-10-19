package anatomy.rdd.extension

/**
 * Created by ganeshchand on 10/18/15.
 * Scala Example to show how to extend Scala's Int and provide square and cube ()
 */
object ScalaImplicitExample {

  // Step 1: Define a class
  class CustomIntFunctions(value: Int){

    // custom functions defined for Int data type
    def square() = value * value

    def cube() = square() * value
  }

  // Step 2: define implicit methods that does the conversion

  object CustomIntFunctions {
    implicit def addCustomIntFunctions(value: Int) = new CustomIntFunctions(value)
  }


  def main(args: Array[String]) {
    val normalIntValue = 10;

    // step 3: Import class

    import CustomIntFunctions.addCustomIntFunctions

    // When you have more than one functions, then you can import using _

    println(normalIntValue.square())
    println(normalIntValue.cube())
  }


}
