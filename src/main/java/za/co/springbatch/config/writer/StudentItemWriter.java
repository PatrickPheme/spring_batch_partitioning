package za.co.springbatch.config.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import za.co.springbatch.model.Student;
import za.co.springbatch.repository.StudentRepository;

import java.util.List;

public class StudentItemWriter implements ItemWriter<Student> {

    private static final Logger logger = LoggerFactory.getLogger(StudentItemWriter.class);

    private final StudentRepository studentRepository;

    public StudentItemWriter(StudentRepository studentRepository) {
        this.studentRepository = studentRepository;
    }

    @Override
    public void write(List<? extends Student> studentlist) throws Exception {
        logger.info("Saving {} students objects", studentlist.size());
        studentRepository.saveAll(studentlist);
    }
}
