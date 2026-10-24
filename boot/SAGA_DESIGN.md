# Saga Orchestrator — Canonical MVP Contract

> Cập nhật: 2026-08-09. Source, tests và
> `../docs/system-contract-inventory.md` là executable truth. Tài liệu này không
> mô tả các topic legacy `shipper.matched`, `no.shipper.available` hay
> `delivery.find-shipper`; chúng đã bị xóa khỏi runtime contract.

## Vai trò

Saga Orchestrator chạy cổng `8095`, persist state/step/outbox trong PostgreSQL và
điều phối order bằng Kafka. Service không tạo order qua REST và không có public
controller. Mọi command được ghi transactional outbox theo aggregate order trước
khi relay sang Kafka.

## Luồng canonical

1. Nhận `order.created`, tạo hoặc replay-safe load Saga.
2. Ghi `saga.command.create-delivery`; Delivery trả
   `delivery.created.result` hoặc `delivery.created.failed`.
3. Chờ nhà hàng quyết định. Chỉ `restaurant.order-confirmed` mới cho phép ghi
   `saga.command.find-shipper`; reject/cancel đi compensation.
4. Mỗi `saga.command.find-shipper` mang `matchingSessionId` do Saga tạo theo
   `MATCHING_STARTED`. Match đọc Redis GEO replica và trả đúng một
   `shipper.found`, hoặc `shipper.not-found` sau retry policy, cùng generation.
5. Với `shipper.found`, Saga ghi `saga.command.cache-shipper-found`; Delivery
   persist offer/expiry rồi phát `delivery.shipper-offered` cho Notification.
6. `delivery.shipper-accepted` gắn shipper vào Saga/Order. Reject, timeout hoặc
   cancel-assignment phát `delivery.shipper-rejected` và rematch có exclusion.
7. `delivery.status-updated` hội tụ state. Terminal success đến từ
   `delivery.completed`; COD settlement do Settlement service xử lý.

## Cancellation và failure

- `order.cancelled` dừng đúng matching generation hiện hành bằng
  `saga.command.stop-matching(orderId, deliveryId, matchingSessionId)` và yêu cầu
  Delivery cancel bằng `saga.command.cancel-delivery`. Stop không có session bị
  từ chối thay vì đoán/broad-cancel; Saga gặp `MATCHING_STARTED` legacy không có
  session sẽ không phát stop.
- Cancel sau pickup phải fail-closed qua `delivery.cancel.failed`; không được báo
  Saga đã cancel trong khi Delivery vẫn giao.
- Không tìm thấy shipper hội tụ Order/Delivery về `SHIPPER_NOT_FOUND` theo state
  guard qua command riêng `saga.command.mark-shipper-not-found`; stale not-found
  không được thắng accept/in-flight state.
- Timeout scheduler đọc batch có giới hạn và xử lý lỗi theo từng aggregate; một
  saga lỗi không được chặn các saga timeout phía sau trong cùng poll.

## Replay boundary

- Event/command active bắt buộc UUID `eventId` và positive aggregate IDs.
- `saga.command.update-order-status` giữ top-level `orderId` và raw
  `originalEvent` cùng correlation; Order từ chối inner orderId mâu thuẫn, JSON
  lỗi, hoặc `SHIPPER_NOT_FOUND` thiếu positive `deliveryId`. Compensation dùng
  delivery identity đã persist trong Saga, không phụ thuộc payload lỗi ban đầu.
- Saga state được pessimistic-lock; exact replay là idempotent, event ID hoặc
  payload mâu thuẫn fail-closed để Kafka retry/DLT.
- Match result phải mang session bằng session trong `MATCHING_STARTED` hiện
  hành; result generation cũ bị bỏ qua. Match fence `stop-before-find` bằng
  durable tombstone `(deliveryId, matchingSessionId)` trước Redis, nên một stop
  cũ không thể hủy rematch mới.
- Outbox giữ ordering theo order. H2/static tests không thay thế PostgreSQL/Kafka
  duplicate, crash-after-commit, restart và DLT rehearsal ở Gate B8.

## Verification

- Focused: listener validation/ACK, SagaManager transitions, outbox transaction,
  Flyway clean/legacy/fail-closed.
- Runtime Gate B8: restaurant-confirm-before-match, one-shipper offer,
  reject/timeout/rematch, cancel before/after pickup, duplicate/crash/restart và
  poison-message DLT trên PostgreSQL/Kafka thật.

## Awaited-reply timeouts

`OFFER_PERSISTING`, `COMPENSATING` and `OFFER_RETIRING` each default to a
120-second reply window. Configure `app.saga.timeout.offer-persisting-seconds`,
`app.saga.timeout.compensating-seconds`, `app.saga.timeout.offer-retiring-seconds`
with `SAGA_OFFER_PERSISTING_TIMEOUT_SECONDS`, `SAGA_COMPENSATING_TIMEOUT_SECONDS`,
`SAGA_OFFER_RETIRING_TIMEOUT_SECONDS`. `app.saga.timeout.max-resends`
(`SAGA_STUCK_STATE_MAX_RESENDS`, default 3) counts resends after the original
command; zero disables resends. Polling uses the existing
`app.saga.timeout-poll-delay-ms` and bounded timeout batch size.

The scheduler locks the case and rechecks its persisted state-entry or last
resend timestamp. Unrelated inbound events do not restart the reply window.
It requeues the latest outbox command for the awaited state's command topic,
keeping the same event ID, payload, key and trace context. An active outbox lease
is allowed to finish before requeueing. Each resend starts another full reply
window; expiry after the last resend marks the case `FAILED` for manual
reconciliation without issuing a new compensation or Order transition.
After commit, `dispatch.stuck.state.failed` (tag `state`) increments and an ERROR
log records the case/order IDs. Inspect case steps and the original outbox
command before reconciling with Delivery; a late reply follows existing state
guards. Flyway V9 backfills legacy state-entry timestamps from `updated_at`.
