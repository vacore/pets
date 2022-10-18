use core::array;
use std::{
	io::{stdout,Write},
	net::UdpSocket,
	slice,
	sync::{
		atomic::{AtomicU32,Ordering},
		Arc,RwLock
	},
	time::Duration,
	thread
};
use rand::prelude::*;

use rustdb::*;

fn main () {
	let t = Arc::new (
		RwLock::new (
			Table::new ()
		)
	);

	let simple_init = || {
		let n=20;
		println! ("Step 1: Adding {n} simple elements to the table to play around\n");

		for id in 0..n {
			let r=Record::gen (id);
			t.write ().unwrap ().add (r).unwrap ();
		}
	};
	simple_init ();

	let tt=Arc::clone (&t);
	thread::spawn (move || {
		println! ("Press Enter to continue");
		unsafe {libc::getchar ()};

		let num=thread_rng ().gen_range (1e6..10e6) as u32;
		println! ("Step 2: Filling the table with {num} elements...");

		let cur=tt.read ().unwrap ().tot ();
		for i in 0..num {
			if i%10000==0 {
				print! ("\ri={i}\x1B[K");
				stdout ().flush ().unwrap ();
			}
			tt.write ().unwrap ().add (Record::gen (i+cur)).unwrap ();
		}
		println! ("\rtot={}", tt.read ().unwrap ().tot ());

		println! ("Press Enter to continue");
		unsafe {libc::getchar ()};

		let n=10;
		println! ("Step 3: Creating {n} artificial writers that will randomly add/rm/upd...");
		let c:[AtomicU32;3] = array::from_fn (|_| AtomicU32::new (0));
		let c=Arc::new (c);

		for i in 0..n {
			let (c,tt) = (Arc::clone (&c), Arc::clone (&tt));
			thread::spawn (move || {
				let freq = thread_rng ().gen_range (3..=100);
				let st   = (1e6/freq as f64).round () as u64;
				println! ("Thread {i}: freq={freq}, st={st}");

				loop {
					let op=thread_rng ().gen_range (0..=2) as usize;
					let id=thread_rng ().gen_range (0..10e6 as u32);
					let res = match op {
						0 => tt.write ().unwrap ().add (Record::gen (id)),
						1 => tt.write ().unwrap ().rm (id),
						2 => tt.write ().unwrap ().upd (Record::gen (id)),
						_=> unreachable! ()
					};

					if res.is_ok () {
						c[op].fetch_add (1, Ordering::SeqCst);
					}

					thread::sleep (Duration::from_micros (st));
				}
			});
		}

		loop {
			thread::sleep (Duration::from_secs (1));

			print! ("\ra={}/s, r={}/s, u={}/s, tot={}\x1B[K",
			        c[0].load (Ordering::SeqCst),
			        c[1].load (Ordering::SeqCst),
			        c[2].load (Ordering::SeqCst),
			        tt.read ().unwrap ().tot ());
			stdout ().flush ().unwrap ();

			c.iter ().for_each (|v| {
				v.store (0, Ordering::SeqCst)
			});
		}
	});

	let socket=UdpSocket::bind ("127.0.0.1:50001").unwrap ();

	loop {
		let mut buf=[0;REQ_SZ];
		let (amt,src) = socket.recv_from (&mut buf).unwrap ();
		assert! (amt==REQ_SZ);

		let p=buf.as_ptr () as *const Request;
		let req: Request = unsafe {
			*p
		};

		let (resp,mut data)=t.read ().unwrap ().fetch (req);

		let resp=&resp as *const Response as *const u8;
		let mut resp = unsafe {
			slice::from_raw_parts (resp, RESP_SZ)
		}.to_vec ();

		resp.append (&mut data);

		socket.send_to (&resp, &src).unwrap ();
	}
}
