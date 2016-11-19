import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;
/**
 * Created by roshni on 11/14/16.
 */

// saves the the two tables - UniprotPDB and ProtMod into ORC format to make it time and memory efficient

public class ORCfile_uniMod {
    private static final org.slf4j.Logger PdbLogger = LoggerFactory.getLogger(ORCfile_uniMod.class);

    public static void main(String[] args)  {

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
                .config("spark.some.config.option", "some-value")
                .getOrCreate();


        // register the UniProt to PDB mapping
        Dataset<Row> uniprotPDB = spark.read()
                .parquet(localDir + "/parquet/uniprotpdb/20161104")
                ;
        System.out.println("size uniprot:" + uniprotPDB.count());
        uniprotPDB.write().format("orc")
                .partitionBy("uniProtId")
                .mode(SaveMode.Overwrite)
                .save("/Users/roshni/GIT/DataframesKavierMapping/uniprotPDB");

       //the protmod table

        Dataset<Row> protMod= spark.read().format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("/Users/roshni/Desktop/ProtMod.csv");
        System.out.println("size protmod:" + protMod.count());

        protMod.write().format("orc").partitionBy("uniProtId_M")
                .mode(SaveMode.Overwrite)
                .save("/Users/roshni/GIT/DataframesKavierMapping/protmod");


    }
}
