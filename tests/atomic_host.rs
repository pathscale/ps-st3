use st3::fanout::{AtomicHost, Host};
use std::{
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

#[test]
fn signal_before_park_leaves_a_permit() {
    let host = Arc::new(AtomicHost::new(1, || 42));
    assert_eq!(host.now_ns(), 42);
    host.unpark(0);
    let (send, receive) = mpsc::channel();
    let worker = thread::spawn(move || {
        host.park(0);
        send.send(()).unwrap();
    });
    receive
        .recv_timeout(Duration::from_secs(5))
        .expect("permit was lost");
    worker.join().unwrap();
}

#[test]
fn repeated_signal_and_wait_handshakes_do_not_lose_wakes() {
    let host = Arc::new(AtomicHost::new(1, || 0));
    let owner = host.clone();
    let (ready, receive_ready) = mpsc::channel();
    let (done, receive_done) = mpsc::channel();
    let worker = thread::spawn(move || {
        for _ in 0..500 {
            ready.send(()).unwrap();
            owner.park(0);
            done.send(()).unwrap();
        }
    });
    for _ in 0..500 {
        receive_ready.recv_timeout(Duration::from_secs(5)).unwrap();
        host.unpark(0);
        receive_done
            .recv_timeout(Duration::from_secs(5))
            .expect("wake was lost");
    }
    worker.join().unwrap();
}
