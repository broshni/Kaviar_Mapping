import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;


/**
 * Created by roshni on 11/9/16.
 */
public class UniprotPDBMod {

    private static final org.slf4j.Logger PdbLogger = LoggerFactory.getLogger(UniprotPDBMod.class);

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
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> uniMod = spark.read().format("orc")
                .load("/Users/roshni/GIT/DataframesKavierMapping/UniProtMod");
        uniMod.createOrReplaceTempView("uni");
        //uni = uni.repartition(10000);
        System.out.println("size uniprot:" + uniMod.count());
        uniMod.show(2);

        System.out.println(uniMod.rdd().getNumPartitions());



       /* // register the UniProt to PDB mapping
        Dataset<Row> uniprotPDB = spark.read()
                .parquet(localDir + "/parquet/uniprotpdb/20161104")
                .sample(false,0.0000001);

        uniprotPDB.createOrReplaceTempView("uniprotPDB");

        System.out.println("size uniprot:" + uniprotPDB.count());

        uniprotPDB = uniprotPDB.repartition(uniprotPDB.col("uniProtId"));

        System.out.println("Example row from PDB to UniProt mapping:");
        uniprotPDB.show(5);

        Dataset<Row> protMod= spark.read().format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("/Users/roshni/Desktop/ProtMod.csv");


         protMod = protMod.repartition(protMod.col("uniProtId_M"));
        protMod.write().mode(SaveMode.Overwrite)
                .format("parquet")
                .save("/Users/roshni/Desktop/dataframes.rcsb.org/parquet/Protmod_p.parquet");

        protMod.createOrReplaceTempView("protMod");

        System.out.println("Size protmod: " + protMod.count());

        System.out.println("Example row from protMod:");
        protMod.show(5);

        // join the ProtMod with Uniprot
        Dataset<Row> uniprotPDBMod= spark.sql("select * from uniprotPDB left join protMod where uniprotPDB.uniProtId = protMod.uniProtId_M and uniprotPDB.uniProtPos=protMod.uniProtPos_M");
        uniprotPDBMod.createOrReplaceTempView("uniprotPDBMod");
        uniprotPDBMod.show(5);
        uniprotPDBMod
                .write()
                .mode(SaveMode.Overwrite)
                .save("/Users/roshni/Desktop/dataframes.rcsb.org/parquet/uniprotPDBMod.parquet");*/
    }
}
