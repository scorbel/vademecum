import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;


public class Master {

	private static String MACHINES_FILE ="machines.txt";
	private static String RUNNING_MACHINES_FILE ="runningMachines.txt";
	private static String INPUT_FILE="input.txt";
	private static String LOGIN = "scorbel";
	private static String OS = null;
	private static String SLAVE_PROJECT_NAME ="slaveShavadoop/bin";
	
	private HashMap<String,Process> umxMachine = new HashMap<String,Process>(); // Dictionnaire identifiant->machine
	private HashMap<String,ArrayList<String>> keyUmx = new HashMap<String,ArrayList<String>>(); // Dictionnaire mot -> {identifiant machine}
	
	public Master() {
		getOsName();
	}
	public static String getOsName()
    {
      if(OS == null) { OS = System.getProperty("os.name"); }
      return OS;
    }
	public static boolean isWindows()
    {
      return getOsName().startsWith("Windows");
    }

	public String getMachinesNameFile() {
		return getUserDir()+"/"+MACHINES_FILE;
	}
	public String getRunnigMachinesNameFile() {
		return getUserDir()+"/"+RUNNING_MACHINES_FILE;
	}
	public String getInputFilename() {
		return getUserDir()+"/"+INPUT_FILE;
	}

	static public String getUserDir()
	{
		String userDir = System.getProperty("user.dir");
		if ( isWindows()) {
			userDir = userDir.replaceAll("\\\\", "/");
		}
		return userDir;

	}
	String[] getSshCmd(String cmd, String slaveName) {
		String[] result=new String[3];
		result[0]="ssh";
		result[2]=cmd;
				
		if (isWindows()) {
			result[1]= slaveName;
		}
		else {
			result[1]=LOGIN+"@"+slaveName;
		}
		return result;
	}
	public int processCmd(String[] cmd, boolean echo, boolean wait) {
		int error =1 ; //no error

	    try {
	    	String s = null;
	        ProcessBuilder pb = new ProcessBuilder(cmd);
	        Process p = pb.start();
	        if (wait) p.waitFor();
            BufferedReader stdInput = new BufferedReader(new
                    InputStreamReader(p.getInputStream()));
    
               BufferedReader stdError = new BufferedReader(new
                    InputStreamReader(p.getErrorStream()));
    
               while ((s = stdInput.readLine()) != null) {
                   if (echo) System.out.println(s);
               }
                
               while ((s = stdError.readLine()) != null) {
                   if (echo) System.out.println(s);
                   error = 0;
               }
               return error;
	
	    } catch (IOException e) {
	        e.printStackTrace();
	    } catch (InterruptedException e) {
	        e.printStackTrace();
	    }
        return 0;

	}

	public int execCmd (String[] cmd, boolean echo){
		String s = null;
		int error =1 ; //no error
		 
        try {
              
            Process p = Runtime.getRuntime().exec(cmd);
            p.waitFor();
             
            BufferedReader stdInput = new BufferedReader(new
                 InputStreamReader(p.getInputStream()));
 
            BufferedReader stdError = new BufferedReader(new
                 InputStreamReader(p.getErrorStream()));
 
            while ((s = stdInput.readLine()) != null) {
                if (echo) System.out.println(s);
            }
             
            while ((s = stdError.readLine()) != null) {
                if (echo) System.out.println(s);
                error = 0;
            }
            return error;
             
        }
        catch (IOException | InterruptedException e) {
            System.out.println("exception happened - here's what I know: ");
            e.printStackTrace();
        }
        return 0;
	   }

	public void  getRunningMachines(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(getRunnigMachinesNameFile())));
		   
		String line;
	    while ((line = reader.readLine()) != null)
	    {
	    	String ordi = line.trim();
			String [] cmd = getSshCmd("hostname", ordi);
	    	if (processCmd(cmd,false,true)==1) {
	    		writer.println(ordi);
	    	}
	    	
	    }
	    reader.close();		
	    writer.close();
	    
	}
	public void callFirstmachine(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		String line;
		if ( (line = reader.readLine()) != null ) {
			String ordi = line.trim();
			if (callMachine(ordi,true)==1) {
				System.out.println("tout est fini");
			}
			
		}
	    reader.close();		

	}
	
	
	public int callMachine(String ordi, boolean wait) {
		String javaCmd = "java -jar "+getUserDir()+"/../"+SLAVE_PROJECT_NAME+"/slave.jar";
		String [] cmd = getSshCmd(javaCmd, ordi);
		return processCmd(cmd,true,wait);
		
	}
	
	public void callAllMachines(String filename) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		String line;
	    while ((line = reader.readLine()) != null)
	    {
			String ordi = line.trim();
			if (callMachine(ordi,true)==1) {
				System.out.println("tout est fini");
			}
	    
	    }
	    reader.close();		
		
	}
		
	public void AM() throws IOException
	{
		BufferedReader reader = new BufferedReader(new FileReader(getInputFilename()));
		// Partie 1: envoie d'une ligne à une machine disponible 
		// On stocke un couple (identifiant, machine) dans le dictionnaire umxMachine
		// En retour on lit un fichier comprenant pour chaque ligne un couple (mot,1)
		String line;
		if ( (line = reader.readLine()) != null ) {
			
		}
		reader.close();

	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Master master = new Master();
		try {
			master.getRunningMachines(master.getMachinesNameFile());
			master.callFirstmachine(master.getRunnigMachinesNameFile());
			//master.AM();

			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

}
