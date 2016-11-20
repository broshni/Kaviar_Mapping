/**
 * Created by roshni on 11/17/16.
 */
import java.io.*;

//Reads the .vcf file for Kaviar and parse it and obtain the table
//in csv formate which can be used for data frames

public class Kaviardataframe {

        public static void main(String[] args) {


            int  lineno =0;
            try {


                // command line parameter
                FileInputStream fstream = new FileInputStream("/Users/roshni/Desktop/Kaviar-160113-Public/vcfs/Kaviar-160113-Public-hg38.vcf");
                // Get the object of DataInputStream
                DataInputStream in = new DataInputStream(fstream);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String strLine; //= "1\t13280035\trs765643056\tA\tG\t.\t.\tAF=0.0000379;AC=1;AN=26378";


                //output file
                File aminoAcid = new File("/Users/roshni/Desktop/KavierTable_Dataframe.csv");




                // if aminoAcid doesnt exists, then create it




                if (!aminoAcid.exists()) {
                    aminoAcid.createNewFile();
                }




                FileWriter fw = new FileWriter(aminoAcid.getAbsoluteFile());
                BufferedWriter output = new BufferedWriter(fw);




                //Read File Line By Line
                System.out.print("Start");
                output.write("chromosomeName,position\n");
                while ((strLine = br.readLine()) != null) {
                    lineno++;


                    if(lineno>13) {
                       //System.out.println(lineno);
                        //System.out.println(strLine);




                       // Parse the file to obtain the Chromosome Name and Positions
                        strLine=strLine.replace(",",":");
                        String[] tempStr = strLine.split("\t");
                        String CName1 = tempStr[0];
                        String CName="chr"+ CName1;
                        //int CNameI=Integer.valueOf(CName);
                        //System.out.println(CName);
                       if(CName.equals("chr11")){
                            System.out.println("True");
                        }
                        int CPos = Integer.valueOf(tempStr[1]);
                        //String ref= tempStr[3];
                        //String alt= tempStr[4];
                        output.write(CName
                                + "," + CPos +  "\n");
                    }


                }
                in.close();
                output.close();
            } catch (IOException e) {//Catch exception if any
                System.err.println("Error: " + e.getMessage());


            }


        }
    }


