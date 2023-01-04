package com.jeffharwell.commoncrawl.warcparser

import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

/*
 * Jeff's Internet Argument Corpus 2.0 FourForum WARC Topic Filter
 *
 * At present this class applies some basic rules to the content of the WARC Record
 * to define a document category, it then allows you to either check for the existence
 * of the categories or to return a list of categories.
 *
 * This is not a general class. This is an implementation of my own topic filter requirements.
 * In particular it is designed to group documents from the Common Crawl dataset into corpora
 * the correspond to the major topic categories in the Internet Argument Corpus 2.0 Four Forum
 * dataset.
 *
 * The categorization algorithm first requires that the core_keyword be found in the document
 * minimummentions number of times. It then requires that any combination of the secondary
 * keywords be found minimummentions number of times. If both of those criteria are met then
 * the document is assigned to this keyword.
 *
 * minmentions for the core keywords defaults to the number of core keywords specified for the topic. Likewise
 * the minimum number of secondary keywords defaults to the number of secondary keywords specified for the topic.
 */
class FourForumsWARCTopicFilter() extends WARCTopicFilter {

  type KeywordStruc = Map[String, List[String]]
  type LookupStruc = Map[Char, List[(String, String, String)]]
  //var stats_writer: Option[A <: StatsWriterTopicFilterStatsWriter] = None
  var stats_writer: Option[StatsWriter] = None
  var stats = false

  // Use this pattern to determine if a core or secondary keyword
  // is completely a subword or not
  val token_separator_pattern: Regex = raw"[\s\p{Punct}-]".r
  var require_token_separator: Boolean = false

  // Changes the behavior of the matcher. By default it will not require a whitespace
  // or punctuation either before or after a matching string, but setting this to true
  // will make the class check to see
  def setRequireTokenSeparator(value: Boolean): Unit = {
    require_token_separator = value
  }

  // True if the object is requiring that the proceeding or following character be either
  // punctuation or whitespace, false otherwise.
  def getRequireTokenSeparator(): Boolean = {
    require_token_separator
  }

  // We are going to be collecting stats and saving them to Cassandra
  // Add the stats writer to the object.
  def setStatsWriter[A <: StatsWriter](writer: A): Unit = {
    if (!stats) {
      stats = true
      stats_writer = Some(writer)
    }
  }

  def writeStats(warc_record: WARCRecord, measurement_type: String, measurement: Long): Unit = {
    // If we have a stats_writer defined, then write the statistic we have been passed
    // otherwise do nothing.
    stats_writer match {
      case Some(w: StatsWriter) => w(warc_record, measurement_type, measurement)
      case _ => None
    }
  }

  def writeStatsCompact(warc_record: WARCRecord, categories: String, processing_time: Long): Unit = {
    // If we have a stats_writer defined, then write the statistic we have been passed
    // otherwise do nothing.
    stats_writer match {
      case Some(w: StatsWriter) => w(warc_record, categories, processing_time)
      case _ => None
    }
  }

 /*
  * This function takes our keywords and creates a lookup structure that is then used
  *  to categorize the keywords and hence categorize the document.
  *
  * Typedef:
  * f: (KeywordStruc, KeywordStruc) => LookupStruc
  *
  * The structure is a Map where the key is the first letter of the keyword, one uppercase key
  * and one lowercase key. The value is a list of tuples; the first member being the keyword
  * we want to match, the second member is the category for the keyword, the third member is the
  * type of keyword, either "core" or "secondary".
  *
  *   Input Structure:
  * Map[String, List[String]](
  * "guncontrol" -> List("strict", "control"),
  * "abortion" -> List("pro-life", "pro-choice"),...)
   *
  * Output Structure:
  * HashMap(
  * 's' -> List(("strict", "guncontrol", "secondary")),
  * 'n' -> List(("natural", "evolution", "secondary")),
  * 'N' -> List(("natural", "evolution", "secondary")),
  * 'P' -> List(
  *   ("pro-choice", "abortion", "secondary"),
  *   ("pro-life", "abortion", "secondary")
  * ),
  * ...)
  *
  * @param core_keywords Map[String, List[String]] categories and core keywords
  * @param secondary_keywords Map[String, List[String]] categories and secondary keywords
  * @return lookup structure Map[Char, List[(String, String, String)]]
  */
  def create_lookup_structure(core_keywords: KeywordStruc, secondary_keywords: KeywordStruc): LookupStruc = {
    // A small generator function, it exists to inject the correct category for an element transformer
    def get_element_transformer(category: String): ((String, List[String])) => List[List[(Char, String, String, String)]] = {
      // The actual element transformer. This takes the category and list of keywords and for each keyword it creates
      // a structure as follows
      // List((first letter upper, keyword, document category, keyword category), (first letter lower, keyword, document category, keyword category))
      def element_transformer(a: (String, List[String])): List[List[(Char, String, String, String)]] = {
        a._2.map(x => List((Character.toLowerCase(x.charAt(0)), x, a._1, category), (Character.toUpperCase(x.charAt(0)), x, a._1, category)))
      }
      element_transformer
    }

    // Create our transformers, one for the secondary category of keywords another for the core category of keywords
    val ets = get_element_transformer("secondary")
    val etc = get_element_transformer("core")

    /* Apply the element transformers to each element in the definition.
     * This creates a structure which looks like the following:
     *
     * List[List[(Character, String, String, String)]] = List(
     *  List(
     *       ('s', "strict", "guncontrol", "secondary"),
     *       ('S', "strict", "guncontrol", "secondary")
     *      ),...)
     *
     * Finally flatten the list, we want to go from
     * List[List[(Character, String, String, String)]]
     * to
     * List[(Character, String, String, String)]
     */
    val b: List[(Char, String, String, String)] = {
      secondary_keywords.map(ets).flatten ++ core_keywords.map(etc).flatten
    }.toList.flatten

    /* Now we want to create the Map where the key is the character, and the
     * value is a list of (keyword, document category, keyword category)
     *
     * foldLeft each element and collect them using a mutable Map. Convert
     * the mutable Map to an immutable Map at the end using toMap.
     */
    b.foldLeft(MMap[Char, List[(String, String, String)]]())((m, element) => {
      // See if the mutable map already has a key for this character
      m.contains(element._1) match {
        // It does, create our tuple of (keyword, document category, keyword category) and
        // create a new list that prepends the new tuple to any existing tuples
        case true => m(element._1) = (element._2, element._3, element._4) :: m(element._1)
        // It does not, create a new list with one element, the tuple
        // (keyword, document category, keyword category)
        case false => m(element._1) = List((element._2, element._3, element._4))
      }
      m
    }).toMap
  }

 /*
 * Creates a structure for quickly looking up the number of times a core or secondary word
 * needs to appear per category.
 *
 * This is the default structure, which requires one appearance for each word specified. I.e. if two
 * core keywords are specified, a document would be required to have at least two of either keyword
 * mentioned before it would pass the filter and be labeled as a member of the corpus for that topic.
 *
 * The criteria structure looks something like.
 *
 * Map[String, Map[String, Int]] = Map("guncontrol" -> Map("core" -> 1, "secondary" -> 2),
 *                                     "abortion" -> Map("core" -> 1, "secondary" -> 2),
 *                                     "evolution" -> Map("core" -> 1, "secondary" -> 4),
 *                                     "existenceofgod" -> Map("core" -> 1, "secondary" -> 3)
 *                                    )
 *
 * @param core_keywords Map[String, List[String]] categories and core keywords
 * @param secondary_keywords Map[String, List[String]] categories and secondary keywords
 * @return criteria structure Map[Char, List[(String, String, String)]]
 */
  def create_criteria_structures(core_keywords: KeywordStruc, secondary_keywords: KeywordStruc): Map[String, Map[String, Int]] = {
    val structure_1: Map[String, Map[String, Int]] = core_keywords.map(x =>
      (x._1, Map("core" -> x._2.size))
    )
    structure_1.map(x =>
      (x._1, x._2 ++ Map("secondary" -> secondary_keywords(x._1).size))
    )
  }

  val core_keywords = Map[String, List[String]](
    "guncontrol" -> List("gun"),
    "abortion" -> List("abortion"),
    "evolution" -> List("evolution"),
    "existenceofgod" -> List("god")
  )

  val secondary_keywords = Map[String, List[String]](
    "guncontrol" -> List("strict", "control"),
    "abortion" -> List("pro-life", "pro-choice"),
    "evolution" -> List("natural", "mechanism", "intelligent", "design"),
    "existenceofgod" -> List("atheist", "theist", "exist")
  )

  val lookup_structure: LookupStruc = create_lookup_structure(core_keywords, secondary_keywords)
  var criteria_structure: Map[String, Map[String, Int]] = create_criteria_structures(core_keywords, secondary_keywords)

  /*
   * Sets the number of minimum mentions for a specific topic.
   *
   * @param topic The string with the topic
   * @param mention A map with the minimum number of core and secondary words
   */
  def setMentions(topic: String, mention: Map[String, Int]): Unit = {
    // You can only set the minimum mentions for a valid topic
    if (!core_keywords.contains(topic) & !core_keywords.contains(topic)) {
      throw new IllegalArgumentException
    }
    // You must specify the minimum mentions for the "core" and "secondary" words
    if (!mention.contains("core") | !mention.contains("secondary")) {
      throw new IllegalArgumentException
    }
    criteria_structure = criteria_structure.foldLeft(MMap[String, Map[String, Int]]())((m, element) => {
      if (topic == element._1) {
        m(element._1) = mention
      } else {
        m(element._1) = element._2
      }
      m
    }).toMap
  }

  /*
* Accepts the WARC Record, unwraps the Option(w.getContent())
* and runs hasCategoriesOption on the content if it exists.
*/
  def hasCategories[A <: WARCRecord](w: A): Boolean = {
    if (getCategories(w).nonEmpty) {
      true
    } else {
      false
    }
  }

  /*
   * Returns a set of categories that the string s matches
   *
   * @param s The string to categorize
   */
  def getCategories[A <: WARCRecord](w: A): Set[String] = {
    w.getContent() match {
      case Some(s) =>
        val start_time = System.nanoTime()
        val categories = categorizeString(s)
        val end_time = System.nanoTime()
        //writeStats(w, "categorization time", end_time - start_time)
        if (categories.nonEmpty) {
          val category_list: Set[String] = for {
                                                  c <- categories
                                               } yield(c)
          val categories_str: String = category_list.toList.mkString(",")
          //categories.foreach(c => writeStats(w, s"category $c", 1))
          //writeStats(w, "assigned a category", 1)
          writeStatsCompact(w, categories_str, end_time - start_time)
        } else {
          //writeStats(w, "assigned a category", 0)
          writeStatsCompact(w, "", end_time - start_time)
        }
        categories
      case None =>
        //writeStats(w, "categorization time", 0)
        //writeStats(w, "assigned a category", 0)
        writeStatsCompact(w,"", 0)
        List[String]().toSet
    }
  }

  /*
   * A more or less pure function, it takes as string as input and some matching 
   * parameters as input and returns a list of categories that match
   *
   * @param s The string of content to categorize
   * @return a list containing the categories that s fits into
   */
  def categorizeString(s: String): Set[String] = {
    categorizeAndCountString(s)._1
  }

  /*
 * A more or less pure function, it takes as string as input and some matching
 * parameters as input and returns a tuple that contains both a list of categories
 * that match (element 1) and the counts of matches by category (element 2).
 *
 * @param s The string of content to categorize
 * @return a type with categories and match counts by category
 */
  def categorizeAndCountString(s: String): (Set[String], Map[String, (Int, Int)]) = {
    // This is the first check
    val all_matches: Map[String, (Int, Int)] = ciMatch(s, lookup_structure)
    //println(all_matches)

    val just_categories = all_matches.foldLeft(ListBuffer[String]())((l, struc) => {
      // l is our accumulator, the list buffer
      // struc is the tuple from the category map returned by ciMatch
      //   so struc._1 is the topic, and struc._2 is the number of core and secondary matches respectively
      if (struc._2._1 >= criteria_structure(struc._1)("core") && struc._2._2 >= criteria_structure(struc._1)("secondary")) {
        l.append(struc._1)
      }
      l
    }).toList

    (just_categories.toSet, all_matches)
  }

  /*
   * This method categorizes the string and returns the categories as a JSON
   * string.
   *
   * For example the categories Set(guncontrol, existenceofgod) would be returned
   * as "{'guncontrol','existenceofgod'}". If there are no categories returned
   * you get the string "{}"
   *
   * @param s The string to categorize
   * @return a string containing a JSON representation of the categories.
   */
  def categorizeAndCountStringReturnString(s: String): String = {
    val categories_and_counts = categorizeAndCountString(s)
    val set_of_categories: Set[String] = categories_and_counts._1
    val string_of_categories = {
      if (set_of_categories.isEmpty) {
        "{}"
      } else {
        s"{'${set_of_categories.mkString("','")}'}"
      }
    }
    string_of_categories
  }

  /* ciMatch
   * 
   * This is a psudo optimized implementation of a caseInsensitive matching algorithm
   * Converted from some plain old nasty Java derived from @icza 's answer to 
   * http://stackoverflow.com/questions/86780/how-to-check-if-a-string-contains-another-string-in-a-case-insensitive-manner-in
   *
   * @param src the string to search for a match
   * @param what the lookup data structure
   * @returns a map of categories and tuples with the first tuple element being the number of core matches
   *          and the second being the number of secondary matches.
   */
  def ciMatch(src: String, what: LookupStruc): Map[String, (Int, Int)] = {
    // the length of our source document in characters
    val source_length = src.size

    // Build the structure that will hold our match counts. It is a mutable map with the topic specified by the string
    // and the number of core and secondary keywords matches are held in the (Int, Int) tuple, which is initialized
    // as (0, 0). We will copy it to a immutable map before we return it.
    val categories = what.foldLeft(MMap[String, (Int, Int)]())((m, struc) => {
      struc._2.foreach(x => {
        if (!m.contains(x._2)) {
          m(x._2) = (0, 0)
        }
      })
      m
    })
    //println(categories)

    // Go through every character of our source document and count the number of matches to the core and secondary
    // keywords specified for each topic. Note that the algorithm is not tokenizing, it just steps through character
    // by character looking for substrings that match. So if a core or secondary keywords shows up in the middle of
    // another word it will still consider it match.
    for (i <- 0 until source_length) {
      val ch = src.charAt(i)
      if (what.contains(ch)) {
        // We have an initial match
        // Now, iterate through each keyword that starts with ch
        what(ch).foreach(x => {
          // This is the length of the string that we are searching for
          val x_1_length = x._1.length

          /*
          if (require_token_separator) {
            print(s"${x._1}: $source_length - $x_1_length = ${source_length - x_1_length}\n")
            print(s"i = $i\n")
            print(s"source_length - x_1_length) >= i: ${(source_length - x_1_length) >= i}\n")
          }
           */
          if (((source_length - x_1_length) >= i) && src.regionMatches(true, i, x._1, 0, x_1_length)) {
            // We have a match, but we are requiring that the match either begin or end
            // with a space, i.e., it can't just be completely a subword match. This is important
            // as it greatly reduces the number of matches for shorter common words like 'gun' for which
            // you want to catch a word like 'shotgun' but not a word like 'segundo'.
            var proceeding_character: String = {
              if (i == 0) {
                // first token in the document matches, what a coincidence
                " "
              } else {
                // get the character right before our starting character
                src.charAt(i - 1).toString
              }
            }
            var following_character: String = {
              if (i + x._1.length >= src.size) {
                // match is at the end of the document, a rare occurrence
                " "
              } else {
                // get the character right after our ending character
                src.charAt(i + x._1.length).toString
              }
            }
            /*
            if (require_token_separator) {
              print(s"Proceeding $proceeding_character, Following $following_character\n")
              print(s"  Proceeding Matches: ${token_separator_pattern.pattern.matcher(proceeding_character).matches()}\n")
              print(s"  Following Matches: ${token_separator_pattern.pattern.matcher(following_character).matches()}\n")
            }
             */
            // Either the following or ending characters must be valid token separator
            // as defined in the compiled regular expression pattern "token_separator_pattern"
            if (!require_token_separator || (token_separator_pattern.pattern.matcher(following_character).matches() ||
                 token_separator_pattern.pattern.matcher(proceeding_character).matches())) {
              /*
              if (require_token_separator) {
                print(s"Looking for matches\n")
              }
               */

              // x._1 is our keyword, x._2 is our document category, x._3 is our keyword category
              if (x._3 == "core") {
                // If the keyword category is core, increment the first value in the category tuple
                categories(x._2) = (categories(x._2)._1 + 1, categories(x._2)._2)
              } else if (x._3 == "secondary") {
                // If the keyword category is secondary, increment the secondary value in the category tuple
                categories(x._2) = (categories(x._2)._1, categories(x._2)._2 + 1)
              } else {
                // We only support "core" and "secondary" keyword categories, don't know what to do with anything else
                throw new RuntimeException(s"Unknown keyword category ${x._3}")
              }
            }
          }
        })
      }
    }
    categories.toMap
  }
}
