package com.delivery.match_service.payload;

import lombok.Getter;

/**
 * ✅ Base Response Wrapper cho tất cả API responses
 * Theo Backend Instructions: LUÔN sử dụng BaseResponse wrapper
 */
@Getter
public class BaseResponse<T> {
    
    private final int status;      // 1 = success, 0 = failure
    private final String message;  // Success/error message
    private final T data;          // Response data
    
    public BaseResponse(int status, T data, String message) {
        this.status = status;
        this.message = message;
        this.data = data;
    }
    
    public BaseResponse(int status, T data) {
        this.status = status;
        this.data = data;
        this.message = status == 1 ? "Thành công" : "Thất bại";
    }
}
