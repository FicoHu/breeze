#[cfg(test)]
mod bit_map_test {
    use ds::BitMap;
    #[test]
    fn test_bit_map() {
        let bits = BitMap::with_capacity(64);
        bits.mark(0);
        bits.mark(1);
        bits.mark(32);
        bits.mark(63);
        let snapshot = bits.snapshot();
        assert_eq!(snapshot.len(), 2);
        assert_eq!(snapshot[0], 0b11);
        assert_eq!(snapshot[1], (1 << 31) | 1);

        bits.unmark(63);
        let snapshot = bits.snapshot();
        assert_eq!(snapshot[1], 1);

        bits.unmark_all(snapshot);
        let snapshot = bits.snapshot();

        assert_eq!(snapshot[0], 0);
        assert_eq!(snapshot[1], 0);
    }
}
