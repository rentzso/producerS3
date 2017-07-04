package insightproject.s3

import java.util.concurrent._
import scala.util.DynamicVariable
/**
  * Created by rfrigato on 6/14/17.
  */
package object producer {
  /**
    * methods used to parallelize operations
    */
  val forkJoinPool = new ForkJoinPool
  class DefaultTaskScheduler {
    def schedule[T](body: => T): ForkJoinTask[T] = {
      val t = new RecursiveTask[T] {
        def compute = body
      }
      forkJoinPool.execute(t)
      t
    }
  }
  val scheduler =
    new DynamicVariable[DefaultTaskScheduler](new DefaultTaskScheduler)

  def task[T](body: => T): ForkJoinTask[T] = {
    scheduler.value.schedule(body)
  }

  /**
    *
    * @param n the number of retries
    * @param fn the body of the code we want to retry
    * @tparam T the result type of the body fn
    * @return the return value of the body fn if it was successful in n retries
    */
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    util.Try { fn } match {
      case util.Success(x) => x
      case _ if n > 1 => {
        Thread.sleep(15000)
        retry(n - 1)(fn)
      }
      case util.Failure(e) => throw e
    }
  }
}
