package com.seizure_w;

import com.seizure.Program;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(path = "api/v1")
public class MainController {
    private static final Logger logger = LogManager.getLogger(MainController.class);
    private Program program = null;

    /*

     REST Resource Naming Guide
     https://www.javadevjournal.com/spring/rest-resource-naming/

     REST Resource Naming Guide
     https://restfulapi.net/resource-naming/

     Resource Naming
     https://www.restapitutorial.com/lessons/restfulresourcenaming.html

     10+ Best Practices for Naming API Endpoints
     https://nordicapis.com/10-best-practices-for-naming-api-endpoints/

     REST Resource Naming Best Practices
     https://medium.com/linkit-intecs/rest-resource-naming-best-practices-cbee65f37a62

    */

    @GetMapping("start")
    public ResponseEntity<?> start() {
        logger.info("trying to start ...");
        program = new Program();
        try {
            program.start(null);
            logger.info("done.");
        } catch (Exception ex) {
            logger.error("start() - {}", ex.toString());
            stop();
            return new ResponseEntity<>(ex.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>("started...", HttpStatus.OK);
    }

    @PostMapping("start")
    public ResponseEntity<?> start(@RequestBody String confContent) {
        logger.info("trying to start ...");
        program = new Program();
        try {
            program.start(confContent);
            logger.info("done.");
        } catch (Exception ex) {
            logger.error("start() - {}", ex.toString());
            stop();
            return new ResponseEntity<>(ex.toString(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>("started...", HttpStatus.OK);
    }

    @GetMapping("stop")
    public ResponseEntity<?> stop() {
        logger.info("trying to stop ...");
        if (program != null) {
            program.stop();
            program = null;
            logger.info("done.");
            return new ResponseEntity<>("stopped...", HttpStatus.OK);
        }
        return new ResponseEntity<>("cannot stop what not started...", HttpStatus.OK);
    }

    @GetMapping("count")
    public long getCount() {
        long count = 0;
        logger.info("trying to extract the count of handled records ...");
        if (program != null) {
            count = program.getRecordsCount();
            logger.info(String.format("count: %s", count));
        }
        return count;
    }
}
