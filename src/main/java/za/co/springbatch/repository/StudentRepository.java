package za.co.springbatch.repository;

import org.springframework.data.repository.CrudRepository;
import za.co.springbatch.model.Student;

public interface StudentRepository extends CrudRepository<Student, Long> {
}
