use kafka_net::byte_buffer_send::{ByteBufferSend, SIZE_HEADER_SIZE};
use kafka_net::channel_state::ChannelState;
use kafka_net::kafka_request::KafkaRequest;
use kafka_net::kafka_response::KafkaResponse;
use kafka_net::network_receive::NetworkReceive;
use kafka_net::request_header::RequestHeader;
use kafka_net::response_header::ResponseHeader;

#[test]
fn test_network_receive_bytes_read() {
    let mut recv = NetworkReceive::with_max_size(128);
    assert_eq!(recv.size(), 0);

    // Feed size header (4 bytes, size=128)
    let size_header = 128i32.to_be_bytes();
    let consumed = recv.feed(&size_header).unwrap();
    assert_eq!(consumed, 4);
    assert_eq!(recv.size(), 4); // only size buffer read
    assert!(!recv.complete());
    assert!(recv.required_memory_amount_known());

    // Feed first 64 bytes of payload
    let chunk1 = vec![0u8; 64];
    let consumed = recv.feed(&chunk1).unwrap();
    assert_eq!(consumed, 64);
    assert_eq!(recv.size(), 68); // 4 (header) + 64 (payload)
    assert!(!recv.complete());

    // Feed remaining 64 bytes
    let chunk2 = vec![0u8; 64];
    let consumed = recv.feed(&chunk2).unwrap();
    assert_eq!(consumed, 64);
    assert_eq!(recv.size(), 132); // 4 + 128
    assert!(recv.complete());
}

#[test]
fn test_network_receive_required_memory_not_known() {
    let recv = NetworkReceive::new();
    assert!(!recv.required_memory_amount_known());
    assert!(!recv.memory_allocated());
}

#[test]
fn test_network_receive_size_header_only() {
    let mut recv = NetworkReceive::new();
    let data = [0, 0, 0, 5u8]; // size = 5
    let consumed = recv.feed(&data).unwrap();
    assert_eq!(consumed, 4);
    assert!(!recv.complete());
    assert!(recv.required_memory_amount_known());
}

#[test]
fn test_network_receive_full_message() {
    let mut recv = NetworkReceive::new();

    // First feed: size header (4 bytes) + partial payload
    let size_header = 3i32.to_be_bytes(); // 3 bytes payload
    let chunk1 = [size_header[0], size_header[1], size_header[2], size_header[3], b'H', b'e'];
    let consumed = recv.feed(&chunk1).unwrap();
    assert_eq!(consumed, 6); // 4 header + 2 payload
    assert!(!recv.complete());

    // Second feed: remaining payload (payload size is 3, already read 2)
    let chunk2 = [b'l', b'l', b'o'];
    let consumed = recv.feed(&chunk2).unwrap();
    assert_eq!(consumed, 1); // only need 1 more byte to complete 3-byte payload
    assert!(recv.complete());

    assert_eq!(recv.payload().unwrap(), b"Hel");
}

#[test]
fn test_network_receive_empty_payload() {
    let mut recv = NetworkReceive::new();
    // size = 0 means empty payload
    let data = [0, 0, 0, 0u8];
    let consumed = recv.feed(&data).unwrap();
    assert_eq!(consumed, 4);
    assert!(recv.complete());
    assert_eq!(recv.payload().unwrap().len(), 0);
}

#[test]
fn test_network_receive_chunked() {
    let mut recv = NetworkReceive::new();

    // Feed size header in two chunks
    recv.feed(&[0u8, 0u8]).unwrap();
    assert!(!recv.complete());
    recv.feed(&[0u8, 5u8]).unwrap();
    assert!(!recv.complete());
    assert!(recv.required_memory_amount_known());

    // Feed payload
    recv.feed(&[b'h', b'e', b'l']).unwrap();
    assert!(!recv.complete());
    recv.feed(&[b'l', b'o']).unwrap();
    assert!(recv.complete());
    assert_eq!(recv.payload().unwrap(), b"hello");
}

#[test]
fn test_network_receive_negative_size() {
    let mut recv = NetworkReceive::new();
    let data = [0xFFu8, 0xFF, 0xFF, 0xFF]; // -1
    let result = recv.feed(&data);
    assert!(result.is_err());
}

#[test]
fn test_network_receive_max_size_exceeded() {
    let mut recv = NetworkReceive::with_max_size(3);
    let data = [0, 0, 0, 5u8]; // size = 5 > max 3
    let result = recv.feed(&data);
    assert!(result.is_err());
}

#[test]
fn test_byte_buffer_send_serialize() {
    let data = b"Hello, Kafka!".to_vec();
    let send = ByteBufferSend::new(data.clone());

    assert_eq!(send.size(), SIZE_HEADER_SIZE + data.len());
    assert!(!send.completed());

    let serialized = send.serialize().unwrap();

    // 4-byte size header + payload
    assert_eq!(serialized.len(), 4 + data.len());
    let size = i32::from_be_bytes(serialized[0..4].try_into().unwrap());
    assert_eq!(size, data.len() as i32);
    assert_eq!(&serialized[4..], &data[..]);
}

#[test]
fn test_byte_buffer_send_empty() {
    let send = ByteBufferSend::new(Vec::new());
    assert_eq!(send.size(), 4); // just the size header
    assert_eq!(send.serialize().unwrap(), [0, 0, 0, 0]);
}

#[test]
fn test_request_header_write_read() {
    let header = RequestHeader::new(1, 12, 42, "my-client");
    let size = header.size();

    let mut buf = Vec::new();
    header.write(&mut buf);
    assert_eq!(buf.len(), size);

    // Verify wire format: apiKey (2) + apiVersion (2) + correlationId (4) + clientIdLen (2) + clientId
    assert_eq!(&buf[0..2], &1i16.to_be_bytes());    // apiKey
    assert_eq!(&buf[2..4], &12i16.to_be_bytes());   // apiVersion
    assert_eq!(&buf[4..8], &42i32.to_be_bytes());   // correlationId
    assert_eq!(&buf[8..10], &9i16.to_be_bytes());   // clientId length
    assert_eq!(&buf[10..19], b"my-client");

    // Round-trip
    let mut slice = buf.as_slice();
    let decoded = RequestHeader::read(&mut slice).unwrap();
    assert_eq!(decoded, header);
    assert!(slice.is_empty());
}

#[test]
fn test_request_header_empty_client_id() {
    let header = RequestHeader::new(0, 0, 0, "");
    let mut buf = Vec::new();
    header.write(&mut buf);

    let mut slice = buf.as_slice();
    let decoded = RequestHeader::read(&mut slice).unwrap();
    assert_eq!(decoded, header);
    assert_eq!(decoded.client_id, "");
}

#[test]
fn test_response_header_write_read() {
    let header = ResponseHeader::new(99);
    let mut buf = Vec::new();
    header.write(&mut buf);

    assert_eq!(buf, 99i32.to_be_bytes());

    let mut slice = buf.as_slice();
    let decoded = ResponseHeader::read(&mut slice).unwrap();
    assert_eq!(decoded, header);
    assert!(slice.is_empty());
}

// Ported from RequestHeaderTest.testRequestHeaderV1
#[test]
fn test_request_header_v1() {
    // V1 header: apiKey(2) + apiVersion(2) + correlationId(4) + clientIdLen(2) + clientId + tags(0)
    // Java test uses empty clientId and expects 10 bytes total.
    // Without flexible versions (v1), the wire format is:
    //   api_key (2) + api_version (2) + correlation_id (4) + client_id_len (2) = 10 bytes for empty client_id
    let header = RequestHeader::new(16, 1, 10, ""); // FIND_COORDINATOR id=16 in Java
    let size = header.size();

    let mut buf = Vec::new();
    header.write(&mut buf);
    assert_eq!(buf.len(), size);

    // Verify exact bytes: apiKey=16, apiVersion=1, correlationId=10, clientIdLen=0
    assert_eq!(&buf[0..2], &16i16.to_be_bytes());
    assert_eq!(&buf[2..4], &1i16.to_be_bytes());
    assert_eq!(&buf[4..8], &10i32.to_be_bytes());
    assert_eq!(&buf[8..10], &0i16.to_be_bytes()); // empty clientId

    // Round-trip
    let mut slice = buf.as_slice();
    let decoded = RequestHeader::read(&mut slice).unwrap();
    assert_eq!(decoded.api_key, 16);
    assert_eq!(decoded.api_version, 1);
    assert_eq!(decoded.correlation_id, 10);
    assert_eq!(decoded.client_id, "");
    assert!(slice.is_empty());
}

// Ported from RequestHeaderTest.parseHeaderFromBufferWithNonZeroPosition
#[test]
fn test_request_header_parse_with_nonzero_position() {
    // Simulate reading header from a buffer that has extra bytes before it
    let header = RequestHeader::new(10, 0, 123, "");

    let mut buf = Vec::new();
    header.write(&mut buf);

    // Prepend some garbage bytes
    let mut with_prefix = vec![0xAA, 0xBB, 0xCC];
    with_prefix.extend_from_slice(&buf);

    // Skip the prefix, read only the header
    let mut slice: &[u8] = &with_prefix[3..];
    let decoded = RequestHeader::read(&mut slice).unwrap();
    assert_eq!(decoded.correlation_id, 123);
    assert_eq!(decoded.api_key, 10);
    assert_eq!(decoded.api_version, 0);
    assert_eq!(decoded.client_id, "");
}

// Ported from NetworkReceiveTest.testSizeWithPredefineBuffer
#[test]
fn test_network_receive_size_with_predefined_buffer() {
    let payload = vec![0u8; 8];
    let recv = NetworkReceive::from_payload("0", payload);

    // Total size = 4 (size buffer) + 8 (payload)
    assert_eq!(recv.size(), 12);
    assert!(recv.complete());
}

// Ported from NetworkReceiveTest.testSizeAfterRead
#[test]
fn test_network_receive_size_after_read_header() {
    let mut recv = NetworkReceive::with_max_size(128);
    let size_header = 32i32.to_be_bytes();
    recv.feed(&size_header).unwrap();

    // After reading header, size = 4 (header) + 0 (payload so far)
    assert_eq!(recv.size(), 4);
}

#[test]
fn test_kafka_request_serialize() {
    let header = RequestHeader::new(1, 0, 1, "client");
    let body = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let req = KafkaRequest::new(header, body);

    let serialized = req.serialize();

    // [size: 4] + [header: 2+2+4+2+6=16] + [body: 4] = 24
    assert_eq!(serialized.len(), 24);

    // Verify size header = header_size + body_size
    let size = i32::from_be_bytes(serialized[0..4].try_into().unwrap());
    assert_eq!(size, 20); // 16 (header) + 4 (body)
}

#[test]
fn test_kafka_response_deserialize() {
    let correlation_id = 42i32;
    let body = [0x01, 0x02, 0x03, 0x04];
    let response_size = 4 + body.len() as i32; // correlationId (4) + body

    let mut data = Vec::new();
    data.extend_from_slice(&response_size.to_be_bytes());
    data.extend_from_slice(&correlation_id.to_be_bytes());
    data.extend_from_slice(&body);

    let response = KafkaResponse::deserialize(&data).unwrap();
    assert_eq!(response.header.correlation_id, 42);
    assert_eq!(&*response.body, &body);
}

#[test]
fn test_kafka_response_too_short() {
    let data = [0u8; 5]; // less than 4+4=8 minimum
    let result = KafkaResponse::deserialize(&data);
    assert!(result.is_err());
}

#[test]
fn test_channel_state_transitions() {
    let state = ChannelState::NotConnected;
    assert!(state.is_not_connected());
    assert!(!state.is_ready());

    let ready = ChannelState::Ready;
    assert!(ready.is_ready());
    assert!(!ready.is_not_connected());

    let auth_failed = ChannelState::AuthenticationFailed { reason: "bad creds".to_string() };
    assert_eq!(auth_failed, ChannelState::AuthenticationFailed { reason: "bad creds".to_string() });
}
