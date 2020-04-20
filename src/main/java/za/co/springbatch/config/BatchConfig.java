package za.co.springbatch.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import za.co.springbatch.config.partitioner.CsvStepPartitioner;
import za.co.springbatch.config.writer.StudentItemWriter;
import za.co.springbatch.model.Student;
import za.co.springbatch.repository.StudentRepository;

import static za.co.springbatch.constants.BatchConstants.*;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private static final Logger logger = LoggerFactory.getLogger(BatchConfig.class);
    public static final Long LONG_OVERRIDE = null;

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final StudentRepository studentRepository;

    public BatchConfig(JobBuilderFactory jobBuilderFactory,
                       StepBuilderFactory stepBuilderFactory,
                       StudentRepository studentRepository) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.studentRepository = studentRepository;
    }

    @Bean
    public Job loadCsvFileJob() {
        return this.jobBuilderFactory.get(LOAD_CSV_FILE_JOB)
                .start(loadCsvStepPartitioner())
                .build();
    }

    private Step loadCsvStepPartitioner() {
        return stepBuilderFactory.get(LOAD_CSV_STEP_PARTITIONER)
                .partitioner(loadCsvStep().getName(), csvStepPartitioner())
                .partitionHandler(loadCsvFileStepPartitionHandler(loadCsvStep(), GRID_SIZE))
                .build();
    }

    private CsvStepPartitioner csvStepPartitioner() {
        return new CsvStepPartitioner();
    }

    private PartitionHandler loadCsvFileStepPartitionHandler(final Step step,
                                                             final int gridSize) {
        TaskExecutorPartitionHandler taskExecutorPartitionHandler =
                new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
        taskExecutorPartitionHandler.setStep(step);
        taskExecutorPartitionHandler.setGridSize(gridSize);
        return taskExecutorPartitionHandler;
    }

    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor =
                new SimpleAsyncTaskExecutor(THREAD_NAME_PREFIX);
        asyncTaskExecutor.setThreadNamePrefix(SLAVE_THREAD);
        asyncTaskExecutor.setConcurrencyLimit(CONCURRENCY_LIMIT);
        return asyncTaskExecutor;
    }

    public Step loadCsvStep() {
        return this.stepBuilderFactory.get(LOAD_CSV_STEP)
                .<Student, Student>chunk(CHUNK_SIZE)
                .reader(flatFileItemReader(LONG_OVERRIDE, LONG_OVERRIDE,
                        LONG_OVERRIDE))
                .writer(writer())
                .build();
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor = new JobRegistryBeanPostProcessor();
        jobRegistryBeanPostProcessor.setJobRegistry(jobRegistry);
        return jobRegistryBeanPostProcessor;
    }


    public StudentItemWriter writer() {
        return new StudentItemWriter(studentRepository);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<Student> flatFileItemReader(
            @Value("#{stepExecutionContext[partition_number]}") final Long partitionNumber,
            @Value("#{stepExecutionContext[first_line]}") final Long firstLine,
            @Value("#{stepExecutionContext[last_line]}") final Long lastLine) {

        logger.info("Partition Number : {}, Reading file from line : {}, to line: {} ", partitionNumber, firstLine, lastLine);

        FlatFileItemReader<Student> reader = new FlatFileItemReader<>();
        reader.setLinesToSkip(Math.toIntExact(firstLine));
        reader.setMaxItemCount(Math.toIntExact(lastLine));
        reader.setResource(new ClassPathResource(STUDENTS_FILENAME));
        reader.setLineMapper(new DefaultLineMapper<Student>() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames("FirstName", "LastName", "Email", "Gender");
                    }
                });

                setFieldSetMapper(new BeanWrapperFieldSetMapper<Student>() {
                    {
                        setTargetType(Student.class);
                    }
                });

            }
        });
        return reader;
    }
}

