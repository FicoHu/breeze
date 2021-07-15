#[cfg(test)]
mod mpsc_test {
    use ds::{channel, Receiver, Sender};
    use std::sync::Arc;
    #[test]
    fn test_mpsc() {
        let (tx, mut rx) = channel(64);
        let tx = Arc::new(tx);
        // enable l0, l1, l2
        let mut data = vec![1, 2, 3];
        for e in &data {
            tx.insert(0, *e);
        }
        let mut recved = Vec::with_capacity(data.len());
        for _ in 0..data.len() {
            recved.push(rx.recv().unwrap());
        }
        assert_eq!(rx.recv(), None);
    }
}
