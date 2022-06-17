package com.jeffharwell.commoncrawl.warcparser

import scala.collection.mutable.{Map => MMap}
import scala.collection.mutable.ListBuffer

/*
 * Jeff's Internet Argument Corpus 2.0 FourForum WARC Categorizer
 *
 * At present this class applies some basic rules to the content of the WARC Record
 * and then allows you to either check for the existence of the categories or to return
 * a list of categories.
 *
 * This is not a general class. This is an implementation of my own categorizer requirements
 * The categorization algorithm first requires that the core_keyword be found in the document
 * minimummentions number of times. It then requires that any combination of the secondary
 * keywords be found minimummentions number of times. If both of those criteria are met then
 * the document is assigned to this keyword.
 *
 * @param minimummentions The minimum number of times a core keyword or combination of secondary
 *                        keywords must appear before the content matches the category. This
 *                        will default to 7.
 */
class FourForumsWARCCategorizer(core_min_mentions: Int, secondary_min_mentions: Int) extends WARCCategorizer {

  type KeywordStruc = Map[String, List[String]]
  type LookupStruc = Map[Char, List[(String, String, String)]]

  /*
 * This function takes our keywords and creates a lookup structure that is then used
 * to categorize the keywords and hence categorize the document.
 *
 * Typedef:
 * f: (KeywordStruc, KeywordStruc) => LookupStruc
 *
 * The structure is a Map where the key is the first letter of the keyword, one uppercase key
 * and one lowercase key. The value is a list of tuples; the first member being the keyword
 * we want to match, the second member is the category for the keyword, the third member is the
 * type of keyword, either "core" or "secondary".
 *
 * Input Structure:
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

  /*
   * Returns a boolean if the string s matches any categories.
   *
   * @param s The string to categorize
   */
  def hasCategories(s: String): Boolean = {
    val categories = categorizeString(s)
    if (categories.size > 0) {
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
  def getCategories(s: String): Set[String] = {
    categorizeString(s)
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
      if (struc._2._1 >= core_min_mentions && struc._2._2 >= secondary_min_mentions) {
        l.append(struc._1)
      }
      l
    }).toList

    (just_categories.toSet, all_matches)
  }

  /* ciMatch
   * 
   * This is a psudo optimized implementation of a caseInsensitive matching algorithm
   * Converted from some plain old nasty Java derived from @icza 's answer to 
   * http://stackoverflow.com/questions/86780/how-to-check-if-a-string-contains-another-string-in-a-case-insensitive-manner-in
   *
   * @param src the string to search for a match
   * @param what the lookup data structure
   * @returns int the number of matches
   */
  def ciMatch(src: String, what: LookupStruc): Map[String, (Int, Int)] = {
    val length: Int = what.size

    val categories = what.foldLeft(MMap[String, (Int, Int)]())((m, struc) => {
      struc._2.foreach(x => {
        if (!m.contains(x._2)) {
          m(x._2) = (0, 0)
        }
      })
      m
    })
    //println(categories)

    // Go through every character of our source document looking for a match to a keyword
    // indicating a category
    for (i <- 0 to src.size - length) {
      val ch = src.charAt(i)
      if (what.contains(ch)) {
        // We have an initial match
        what(ch).foreach(x => {
          // Iterate through each keyword that starts with ch
          if (src.regionMatches(true, i, x._1, 0, x._1.length)) {
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
        })
      }
    }
    categories.toMap
  }
}
