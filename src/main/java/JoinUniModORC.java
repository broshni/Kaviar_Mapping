import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;

/**
 * Created by roshni on 11/15/16.
 */

// joins the two table - UniprotPDB and ProtMod to form UniProtMod and saving it in ORC format.

public class JoinUniModORC {
    private static final org.slf4j.Logger PdbLogger = LoggerFactory.getLogger(ORCfile_uniMod.class);

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);


        //toplevel folder where wget wrote the data to
        String localDir = "/Users/roshni/Desktop/Dataframes/dataframes.rcsb.org";

        // assumes the human genetic data is available as a parquet file
        // also needs the uniprot-PDB mapping parquet file

        int cores = Runtime.getRuntime().availableProcessors();

        System.out.println("available cores: " + cores);
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[" + cores + "]")
                .getOrCreate();


        Dataset<Row> uni = spark.read().format("orc")
                .load("/Users/roshni/GIT/DataframesKavierMapping/uniprotPDB");
        uni = uni.repartition(10000);
        uni.createOrReplaceTempView("uni");
        System.out.println("size uniprot:" + uni.count());
        //uni.show(2);

        System.out.println(uni.rdd().getNumPartitions());


        Dataset<Row> mod = spark.read().format("orc")
                .load("/Users/roshni/GIT/DataframesKavierMapping/protmod");
        mod = mod.repartition(10000);
        mod.createOrReplaceTempView("mod");
        System.out.println("size protmod:" + mod.count());
        //mod.show(2);
        System.out.println(mod.rdd().getNumPartitions());


        // join the ProtMod with Uniprot
        Dataset<Row> uniprotPDBMod= spark.sql("select * from uni left join mod " +
                "where uni.uniProtId = mod.uniProtId_M and uni.uniProtPos=mod.uniProtPos_M");

        uniprotPDBMod.write().format("orc").partitionBy("uniProtId")
                .mode(SaveMode.Overwrite)
                .save("/Users/roshni/GIT/DataframesKavierMapping/UniProtMod");

        uniprotPDBMod.show(5);
    }
}
