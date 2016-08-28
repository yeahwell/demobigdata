package com.webmovie.bigdata.springboot.web;

import java.util.Date;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@EnableAutoConfiguration
@RequestMapping("/demo01")
public class Demo01Controller {

	@RequestMapping("/index")
	public String home() {
		return "Hello World!";
	}

	@RequestMapping("/now")
	public String hehe() {
		return "现在时间：" + (new Date()).toString();
	}

}
