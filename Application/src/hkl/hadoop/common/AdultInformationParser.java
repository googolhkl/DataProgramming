package hkl.hadoop.common;

import org.apache.hadoop.io.Text;

public class AdultInformationParser 
{
	// field
	private int age;					//use
	private String workClass;    	//use
	//private int fnlwgt;
	private String education;		//use
	//private int educationNum;
	private String maritalStatus;	//use
	private String occupation;		//use
	//private String relationship;
	private String race;				//use
	private String sex;				//use
	//private int capitalGain;
	//private int capitalLoss;
	private int hoursPerWeek;		//use
	private String nativeCountry;	//use
	private String pay;				//use	
	
	public AdultInformationParser(Text text)
	{
		try
		{
			String[] colums = text.toString().split(",");
			
			age = Integer.parseInt(colums[0].trim()); 
			workClass = colums[1].trim(); 
			education = colums[3].trim(); 
			maritalStatus = colums[5].trim(); 
			occupation = colums[6].trim(); 
			race = colums[8].trim();
			sex = colums[9].trim();
			hoursPerWeek = Integer.parseInt(colums[12].trim());
			nativeCountry = colums[13].trim();
			pay = colums[14].trim();			
		}
		catch(Exception e)
		{
			System.out.println("Error : " + e.getMessage());
		}
		finally
		{
			//empty
		}
	}
	//getter
	public int getAge() { return age;}
	public void setAge(int num) { age = num;}
	public String getWorkClass() { return workClass;}
	public String getEducation() {return education;}
	public String getMaritalStatus() { return maritalStatus;}
	public String getOccupation() { return occupation;}
	public String getRace() {return race;}
	public String getSex() {return sex;}
	public int getHoursPerWeek() { return hoursPerWeek; }
	public String getNativeCountry() { return nativeCountry;}
	public String getPay() { return pay;}
}