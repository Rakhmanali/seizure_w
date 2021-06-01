package com.seizure_w;

import com.seizure.Program;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "api/v1")
public class MainController {

    private Program program = null;

    @GetMapping("start")
    public String start() {
        program = new Program();
        program.start();
        return "start";
    }

    @GetMapping("stop")
    public String stop() {
        if (program != null) {
            program.stop();
        }
        return "stop";
    }
}
