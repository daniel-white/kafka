use kafka_compress::compression::Compression;
use kafka_record::CompressionType;

const TEST_DATA: &[u8] = b"The quick brown fox jumps over the lazy dog.\
The quick brown fox jumps over the lazy dog.\
The quick brown fox jumps over the lazy dog.";

const SMALL_DATA: &[u8] = b"Hello, Kafka!";

#[test]
fn test_compress_decompress_none() {
    let compressed = Compression::compress(TEST_DATA, CompressionType::NONE, 0).unwrap();
    assert_eq!(compressed, TEST_DATA);

    let decompressed = Compression::decompress(&compressed, CompressionType::NONE, None).unwrap();
    assert_eq!(decompressed, TEST_DATA);
}

#[test]
fn test_compress_decompress_gzip_round_trip() {
    let compressed = Compression::compress(TEST_DATA, CompressionType::GZIP, 6).unwrap();
    assert_ne!(compressed, TEST_DATA);
    assert!(compressed.len() < TEST_DATA.len());

    let decompressed = Compression::decompress(&compressed, CompressionType::GZIP, None).unwrap();
    assert_eq!(decompressed, TEST_DATA);
}

#[test]
fn test_compress_decompress_gzip_default_level() {
    let compressed = Compression::compress(TEST_DATA, CompressionType::GZIP, -1).unwrap();
    let decompressed = Compression::decompress(&compressed, CompressionType::GZIP, None).unwrap();
    assert_eq!(decompressed, TEST_DATA);
}

#[test]
fn test_compress_decompress_gzip_empty() {
    let compressed = Compression::compress(&[], CompressionType::GZIP, 6).unwrap();
    let decompressed = Compression::decompress(&compressed, CompressionType::GZIP, None).unwrap();
    assert_eq!(decompressed, b"");
}

#[test]
fn test_compress_decompress_snappy_round_trip() {
    let compressed = Compression::compress(TEST_DATA, CompressionType::SNAPPY, 0).unwrap();
    let decompressed = Compression::decompress(&compressed, CompressionType::SNAPPY, None).unwrap();
    assert_eq!(decompressed, TEST_DATA);
}

#[test]
fn test_compress_decompress_snappy_small() {
    let compressed = Compression::compress(SMALL_DATA, CompressionType::SNAPPY, 0).unwrap();
    let decompressed = Compression::decompress(&compressed, CompressionType::SNAPPY, None).unwrap();
    assert_eq!(decompressed, SMALL_DATA);
}

#[test]
fn test_compress_decompress_lz4_round_trip() {
    let compressed = Compression::compress(TEST_DATA, CompressionType::LZ4, 9).unwrap();
    let decompressed = Compression::decompress(&compressed, CompressionType::LZ4, None).unwrap();
    assert_eq!(decompressed, TEST_DATA);
}

#[test]
fn test_compress_decompress_lz4_small() {
    let compressed = Compression::compress(SMALL_DATA, CompressionType::LZ4, 9).unwrap();
    let decompressed = Compression::decompress(&compressed, CompressionType::LZ4, None).unwrap();
    assert_eq!(decompressed, SMALL_DATA);
}

#[test]
fn test_compress_decompress_zstd_round_trip() {
    let compressed = Compression::compress(TEST_DATA, CompressionType::ZSTD, 3).unwrap();
    assert!(compressed.len() < TEST_DATA.len());

    let decompressed = Compression::decompress(&compressed, CompressionType::ZSTD, None).unwrap();
    assert_eq!(decompressed, TEST_DATA);
}

#[test]
fn test_compress_decompress_zstd_default_level() {
    let compressed = Compression::compress(TEST_DATA, CompressionType::ZSTD, 0).unwrap();
    let decompressed = Compression::decompress(&compressed, CompressionType::ZSTD, None).unwrap();
    assert_eq!(decompressed, TEST_DATA);
}

#[test]
fn test_compress_decompress_zstd_empty() {
    let compressed = Compression::compress(&[], CompressionType::ZSTD, 3).unwrap();
    let decompressed = Compression::decompress(&compressed, CompressionType::ZSTD, None).unwrap();
    assert_eq!(decompressed, b"");
}

#[test]
fn test_compression_supports_levels() {
    assert!(!Compression::supports_levels(CompressionType::NONE));
    assert!(Compression::supports_levels(CompressionType::GZIP));
    assert!(!Compression::supports_levels(CompressionType::SNAPPY));
    assert!(Compression::supports_levels(CompressionType::LZ4));
    assert!(Compression::supports_levels(CompressionType::ZSTD));
}

#[test]
fn test_compression_default_levels() {
    assert_eq!(Compression::default_level(CompressionType::NONE), 0);
    assert_eq!(Compression::default_level(CompressionType::GZIP), -1);
    assert_eq!(Compression::default_level(CompressionType::SNAPPY), 0);
    assert_eq!(Compression::default_level(CompressionType::LZ4), 9);
    assert_eq!(Compression::default_level(CompressionType::ZSTD), 3);
}

#[test]
fn test_gzip_produces_valid_gzip_header() {
    let compressed = Compression::compress(TEST_DATA, CompressionType::GZIP, 6).unwrap();
    // Gzip magic bytes: 0x1f 0x8b
    assert_eq!(&compressed[0..2], &[0x1f, 0x8b]);
    // Compression method: 8 (deflate)
    assert_eq!(compressed[2], 8);
}

#[test]
fn test_zstd_produces_valid_zstd_magic() {
    let compressed = Compression::compress(TEST_DATA, CompressionType::ZSTD, 3).unwrap();
    // Zstd magic number: 0x28 0xB5 0x2F 0xFD (little-endian)
    assert_eq!(&compressed[0..4], &[0x28, 0xB5, 0x2F, 0xFD]);
}
