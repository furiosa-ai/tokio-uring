use std::{env, net::SocketAddr};

use tokio_uring::{net::TcpStream, Buffer, Submit};

fn main() {
    let args: Vec<_> = env::args().collect();

    if args.len() <= 1 {
        panic!("no addr specified");
    }

    let socket_addr: SocketAddr = args[1].parse().unwrap();

    tokio_uring::start(async {
        let stream = TcpStream::connect(socket_addr).await.unwrap();
        let buffer = Buffer::new(vec![1u8; 128]);
        let buf = buffer;

        let (n, buf) = stream.write(buf).submit().await.unwrap();
        println!("written: {}", n);

        let (read, buf) = stream.read(buf).await.unwrap();
        println!("read: {:?}", &buf[0][..read]);
    });
}
