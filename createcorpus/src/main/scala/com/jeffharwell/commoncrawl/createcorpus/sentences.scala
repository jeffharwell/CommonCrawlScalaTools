package com.jeffharwell.commoncrawl.createcorpus

import java.util.Properties
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.pipeline.Annotation
import edu.stanford.nlp.util.CoreMap
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation

// See: https://stackoverflow.com/questions/495741/iterating-over-java-collections-in-scala
import scala.collection.JavaConversions._

//import edu.stanford.nlp.ling.CoreAnnotation
//import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation
//import edu.stanford.nlp.ling.CoreLabel

object sentences {
  def main(args: Array[String]): Unit = {
    var props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit")

    var pipeline = new StanfordCoreNLP(props)
    var annotation = new Annotation("This is a sentence. This is another sentence. And Another?")
    pipeline.annotate(annotation)

    var sentences: java.util.List[CoreMap] = annotation.get(classOf[SentencesAnnotation])
    sentences.foreach {
      println(_)
    }
  }
}
