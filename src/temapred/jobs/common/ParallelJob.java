package temapred.jobs.common;

import org.apache.hadoop.mapreduce.Job;


public abstract class ParallelJob implements IJobs {
	private Job myJob;
	public void setJob(Job thisJob){
		myJob = thisJob;
	}
	
	public Job getJob(){
		return myJob;
	}
}
