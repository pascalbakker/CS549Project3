import java.util.Random;
import java.io.FileWriter;   // Import the FileWriter class
import java.io.IOException;  // Import the IOException class to handle errors

public class DataGenerator {
   public static void main(String[] args) throws IOException  {
      
      FileWriter myWriter = new FileWriter("points.txt");
      for (int i = 1; i<=11500000; i++) {      
         Point point = new Point();
         String text = point.x +","+point.y+"\n";
            myWriter.write(text);
      }
      myWriter.close();
   }
 }
 

 class Point {
   int x;
   int y;
   
   Point () {
      Random random = new Random();
      x = random.nextInt(10000) + 1;
      y = random.nextInt(10000) + 1;
   }   

 }