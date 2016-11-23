public class ControllerCheck {

   public static int myvalue = 0;
   public static void main(String args[]) {
	  Control R1 = new Control( "T1",myvalue);
      R1.start();
      
      Control R2 = new Control( "T2",myvalue);
      R2.start();
   }   
}