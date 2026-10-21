package com.delivery.match_service.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.ArrayList;
import java.util.List;

/**
 * ✅ Web MVC Configuration để handle multiple content types
 * Theo Backend Instructions: Proper configuration classes
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {
    
    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        // ✅ Add Jackson converter that can handle text/plain as JSON
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        
        // Support both application/json và text/plain
        List<MediaType> supportedMediaTypes = new ArrayList<>();
        supportedMediaTypes.add(MediaType.APPLICATION_JSON);
        supportedMediaTypes.add(MediaType.TEXT_PLAIN);
        supportedMediaTypes.add(MediaType.ALL);
        
        converter.setSupportedMediaTypes(supportedMediaTypes);
        converters.add(converter);
    }
}
