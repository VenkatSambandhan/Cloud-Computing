package indiana.cgl.hadoop.pagerank.helper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
public class quantifyDiff {
	public static void main(String[] args) throws IOException {
	    Scanner console = new Scanner(System.in); 
	    //System.out.print("Type first file name to use: ");
	    //String filename1 = console.nextLine();
	    //System.out.print("Type second file name to use: ");
	    //String filename2 = console.nextLine();
	    //Scanner input1 = new Scanner(new File("/home/summer/share/HadoopPageRankLocal/HadoopPageRank/PageRankDataGenerator/correct_output.3000url.txt"));
	    //Scanner input2 = new Scanner(new File("/home/summer/share/HadoopPageRankLocal/HadoopPageRank/_user_summer_output_part-r-00000"));
	    FileReader fileReader1 = 
                new FileReader("/home/summer/share/HadoopPageRankLocal/HadoopPageRank/PageRankDataGenerator/correct_output.3000url.txt");
	    FileReader fileReader2 = 
                new FileReader("/home/summer/share/HadoopPageRankLocal/HadoopPageRank/_user_summer_output_part-r-00000");
	    BufferedReader bufferedReader1 = new BufferedReader(fileReader1);
	    BufferedReader bufferedReader2 = new BufferedReader(fileReader2);
	    String line1,line2;
	    String[] hold;
	    double d1,d2;
	    int i = 0;
	    //System.out.println("\t\t Saliya\t\t\t\t Maaz\t\t\t\t DIFFERENCE");
	    while(((line1 = bufferedReader1.readLine()) != null)&&((line2 = bufferedReader2.readLine()) != null)) {
	    	hold = line1.split("\t");
	    	d1 = Double.parseDouble(hold[1]);
	    	hold = line2.split("\t");
	    	d2 = Double.parseDouble(hold[1]);
            System.out.println(i+":\t\t "+d1+"\t\t "+d2+"\t\t "+(d1-d2));
            i++;
        }   

        // Always close files.
        bufferedReader1.close();
        bufferedReader2.close(); 
	    
	    //String s1 = input1.next();
	    //String s2 = input2.next();
	    
	    double s3 = 0;
	    double s4 = 0;
	    boolean similar=true;
	    //System.out.print(s1+" Type "+s2);
	    /*while(input1.hasNext()&&input2.hasNext()){
	    	s3 = Double.parseDouble(s1);
	    	s4 = Double.parseDouble(s2);
	    	System.out.println(s3+" "+s4+" "+(s3-s4));
	    } */
	}
}
