import scala.util.matching.Regex

/**
 * MetaCharacters:
 */
 // ^ - begining of the line
// $ - begining of the line
// Character Classes: [] e.g [Bb][Uu][Ss][Hh] // match all cases of bush

// ^[Ii] am // any line that starts with I am. I can be upper or lower case

// range: [a-z] [a-zA-Z]
  // [0-9][a-zA-Z] match a line tha starts with a single digit followed by any words

// [^?.]$ -- when ^ is used inside the character class, it negates the match

// . => any character




val LogEntry = """Completed in (\d+)ms \(View: (\d+), DB: (\d+)\) \| (\d+) OK \[http://app.domain.com(.*)\?.*""".r


val line = "Completed in 100ms (View: 25, DB: 75) | 200 OK [http://app.domain.com?params=here]"

val LogEntry(totalTime, viewTime, dbTime, responseCode, uri) = line


