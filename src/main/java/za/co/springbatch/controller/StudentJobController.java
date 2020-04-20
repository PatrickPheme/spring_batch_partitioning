package za.co.springbatch.controller;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static za.co.springbatch.constants.BatchConstants.LOAD_CSV_FILE_JOB;

@RestController()
@RequestMapping("students")
public class StudentJobController {

    private final JobLauncher jobLauncher;
    private final JobRegistry jobRegistry;

    public StudentJobController(JobLauncher jobLauncher,
                                JobRegistry jobRegistry) {
        this.jobLauncher = jobLauncher;
        this.jobRegistry = jobRegistry;
    }

    @RequestMapping(method = GET, value = "load")
    public ResponseEntity loadStudents() throws Exception{
        JobExecution jobExecution = jobLauncher.run(jobRegistry.getJob(LOAD_CSV_FILE_JOB), new JobParameters());
        return ResponseEntity.ok("Job with Id : "+ jobExecution.getJobId()  +", Successfully Started With Status : " +  jobExecution.getStatus().name());
    }
}
