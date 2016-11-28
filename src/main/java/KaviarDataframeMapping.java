import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;
import static org.apache.spark.sql.functions.col;

/**
 * Created by roshni on 11/8/16.
 */
public class KaviarDataframeMapping {
    private static final org.slf4j.Logger PdbLogger = LoggerFactory.getLogger(KaviarDataframeMapping.class);
    public static void main(String[] args) {

        //toplevel folder where wget wrote the data to
        String localDir = "/Users/roshni/Desktop/Dataframes/dataframes.rcsb.org";

        // assumes the human genetic data is available as a parquet file
        // also needs the uniprot-PDB mapping parquet/ORC file. Also Kaviar as ORC File.

        int cores = Runtime.getRuntime().availableProcessors();

        System.out.println("available cores: " + cores);
       
        //Create a spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[" + cores + "]")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        //read the kavier table
        Dataset<Row> Kavier1= spark.read().format("ORC")
                .load("/Users/roshni/GIT/DataframesKavierMapping/KaviarDataframe");
        Kavier1.createOrReplaceTempView("K1");


        //select a particular chromosome: chr11 in this case
        Dataset<Row> KaviarChr11 = spark.sql("select * from K1 where chromosomeName = 'chr11’”);
        //KaviarChr11.withColumnRenamed("position","positionK");
        KaviarChr11.printSchema();
        //KaviarChr11= KaviarChr11.repartition(10000);
        System.out.println("Kaviar : " + KaviarChr11.count());
        KaviarChr11.createOrReplaceTempView("Kchr11");



        //load the mapping from the human genome (assembly 38) to UniProt       Dataset<Row> chr11 = spark.read().parquet(localDir + "/parquet/humangenome/20161105/hg38/chr11”);
        chr11= chr11.filter(col("uniProtPos").gt(0));
        //chr11= chr11.repartition(10000);
        chr11.createOrReplaceTempView("chr11");
        System.out.println("chr11 : " + chr11.count());
        /*chr11 = chr11.filter(new FilterFunction<Row>() {
            public boolean call(Row row) throws Exception {

                return row.getInt(13) > 0;
            }
        });*/
        //chr11.show(10);

        //Map kaviar to genome

      Dataset<Row> Kaviertogenome = spark.sql("select * from Kchr11 left join chr11 " +
               "where Kchr11.chromosomeName = chr11.chromosome and Kchr11.position = chr11.position");
        //Kaviertogenome = Kaviertogenome.repartition(10000);
        System.out.println("chr11toKaviar : " + Kaviertogenome.count());
       Kaviertogenome.createOrReplaceTempView("KtoG");

        // register the UniProt to PDB mapping
        Dataset<Row> uniprotPDB = spark.read().parquet(localDir+"/parquet/uniprotpdb/20161104");
        //uniprotPDB= uniprotPDB.repartition(10000);
        uniprotPDB.createOrReplaceTempView("uniprotPDB");
        System.out.println("Example row from PDB to UniProt mapping:"); //+ uniprotPDB.count());
        //uniprotPDB.show(1);

       //Combine to obtain the pdb info

        Dataset<Row> chrKuni = spark.sql("select * from KtoG left join uniprotPDB " +
                "where KtoG.uniProtId= uniprotPDB.uniprotId and KtoG.UniProtPos= uniprotPDB.uniProtPos");
        //chrKuni = chrKuni.repartition(10000);

	//show the number of unique pdb id Kaviar is mapping
	Dataset<Row> p = chrKuni.agg(approxCountDistinct("pdbId"));
        p.show();

	//number of kaviar positions which get mapped to the PDB
	Dataset<Row> pos = spark.sql("select distinct positionk from chrKuni");

        System.out.println("Ktochr11toUni : " + pos.count());

        //System.out.println("Ktochr11toUni : " + chrKuni.count());
        //System.out.println(chrKuni.distinct());
        //chrKuni.groupBy("uniProtId").count().show();


        //Register the protmod table
        //Dataset<Row> uniprotMod = spark.read().format("ORC")
                //.load("/Users/roshni/GIT/DataframesKavierMapping/UniProtMod");
        Dataset<Row> mod = spark.read().format("orc")
                .load("/Users/roshni/GIT/DataframesKavierMapping/protmod");
        //mod= mod.repartition(10000);
        mod.createOrReplaceTempView("mod");
        System.out.println("Example row from Mod mapping:"); //+ mod.count());


        //Combine for protein modification

        Dataset<Row> chrKMod = spark.sql("select * from KtoG left join mod " +
                "where KtoG.uniProtId= mod.uniProtId_M and KtoG.UniProtPos= mod.uniProtPos_M");

        System.out.println("KtochrYtoMod : " + chrKMod.count());
        chrKMod.groupBy("Modification").count().show();





    }
}
