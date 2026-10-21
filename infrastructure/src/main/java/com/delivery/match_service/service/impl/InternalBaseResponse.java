package com.delivery.match_service.service.impl;

public record InternalBaseResponse<T>(int status, String message, T data) {
}
