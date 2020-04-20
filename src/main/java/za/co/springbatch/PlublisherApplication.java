package za.co.springbatch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@EnableAsync
@SpringBootApplication
public class PlublisherApplication {

	public static void main(String[] args) {
		SpringApplication.run(PlublisherApplication.class, args);
	}
}
