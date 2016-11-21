import org.apache.spark.SparkContext


case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean);

object HelloScala
{

    def toDouble(s : String) = {
        if ("?".equals(s))
            Double.NaN
        else
            s.toDouble
    }

    def parse (line: String) = {
        val pieces = line.split(',');
        val id1 = pieces(0).toInt;
        val id2 = pieces(1).toInt;
        val scores = pieces.slice(2, 11).map(toDouble)
        val matched = pieces(11).toBoolean
        MatchData(id1, id2, scores, matched)
    }


    def main(args:Array[String]):Unit =
    {
        val sc = SparkContext
        val rawblocks= sc.textFile("/Users/wuyiran/Documents/workspace/data/linakge");
        println ("I like scala");
    }
}