#![allow (non_snake_case)]

use std::{
	io::{self,Read},
	mem,
	net::{
		Ipv4Addr,
		SocketAddrV4,
		UdpSocket
	},
	slice,
	str,
};

use rustdb::*;

const USAGE:&str = r"Commands:
  q - sort by column 1 (id)
  w - sort by column 2 (num)
  e - sort by column 3 (s)
  up/down     - 1 element  up/down
  PgUp/PgDown - N elements up/down
  Home/End    - to first/to last
  0..9: change knob position      ";

const NREC:usize = 10;   // screen height in terms of rows

#[derive (Debug)]
struct Client {
	addr: SocketAddrV4,
	sock: UdpSocket   ,
	data: Request     ,
	npos: u32         ,  // number of possible screen positions
	kl  : [u32;NREC]     // knob limits (in terms of screens)
}

#[derive (Debug)]
enum Cmd {
	Pos (u32)     ,  // 0..9
	Col (u32)     ,  // sorting column
	OneRow (bool) ,  // Up/Down
	OnePage (bool),  // PageUp/PageDown
	Home (bool)      // Home/End
}

impl Client {
	fn new ()->Self {
		Self {
			sock: UdpSocket::bind ("0.0.0.0:0").unwrap ()             ,
			addr: SocketAddrV4::new (Ipv4Addr::new (127,0,0,1), 50001),
			data: Request {
				col : Column::Id ,
				bw  : false      ,
				N   : NREC as u32,
				CS  : 0          ,
				NS  : 1          ,
			},
			npos: 0        ,
			kl  : [0; NREC]
		}
	}

	fn submit (&mut self, cmd:Cmd) {
		let r=&mut self.data;

		use Cmd::*;
		match cmd {
			Pos (pos) => {                               // new knob position [0..9]
				if pos < self.npos {
					r.CS = self.kl[pos as usize];
				}
				else {
					return
				}
			}
			Col (col) => {                               // new column to sort
				if r.col as u32==col {
					r.bw ^= true;
				}
				r.col = match col {
					0=>Column::Id,
					1=>Column::Num,
					2=>Column::Str,
					_=>unreachable! ()
				};
			}
			OneRow (up) => {                             // Up/Down 1-row
				if up {
					if r.CS>0 {
						r.CS -= 1 ;
					}
				}
				else {
					if r.CS < r.NS-1 {
						r.CS += 1;
					}
				}
			}
			OnePage (up) => {                           // Up/Down Page
				if up {
					if r.CS < r.N-1 {
						r.CS=0
					}
					else {
						r.CS -= r.N-1;
					}
				}
				else {
					if r.CS+r.N > r.NS {
						r.CS = r.NS-1;
					}
					else {
						r.CS += r.N-1;
					}
				}
			}
			Home (up) => r.CS = if up {0} else {r.NS-1}  // Home/End
		}

		self.fire ();
	}

	fn fire (&mut self) {
		/* Send request */
		let p=&self.data as *const Request as *const u8;
		let bytes = unsafe {
			slice::from_raw_parts (p, mem::size_of::<Request> ())
		};
		self.sock.send_to (bytes, &self.addr).unwrap ();

		/* Receive Response */
		let mut buf = vec![0;RESP_SZ + REC_SZ*self.data.N as usize];
		let (amt,_) = self.sock.recv_from (&mut buf).unwrap ();

		let Nrec=((amt-RESP_SZ)/REC_SZ) as u32;
		let (resp, recs) = buf.split_at (RESP_SZ);

		let p=resp.as_ptr () as *const Response;
		let resp = unsafe { &*p };

		/* Process Response */
		if resp.ok {
			let (CS,NS,N) = (resp.CS,resp.NS,self.data.N);

			/* Fill knob data */
			self.npos = if NS>N {N} else {NS};
			for i in 1..self.kl.len () {
				self.kl[i] = (i as f32 * (NS as f32 -1.0) / (self.npos as f32 -1.0))
				           .round () as u32;
			}

			/* Find after which pos CS is */
			let mut pos=0;
			for i in (0..self.npos).rev () {
				if CS >= self.kl[i as usize] {
					pos=i;
					break;
				}
			};

			/* Setup knob drawing */
			let kl=if N+1>NS {N-NS+1} else {1};
			let mut knob = ['░';NREC];
			for i in 0..N {
				if i>=pos && i<pos+kl {
					knob[i as usize] = '▒';
				}
			}
			let mut a = CS + 1;
			let mut b = CS + N;
			let   tot = NS + N - 1;
			if self.data.bw {
				a = tot-a+1;
				b = tot-b+1;
			}
			println! ("Showing elements: ({a}..{b})/{tot}");

			use Column::*;
			let mut s = [' '; NumCol as usize];
			for (i,s) in s.iter_mut ().enumerate () {
				if self.data.col as usize == i {
					*s = if self.data.bw {'↑'} else {'↓'};
				}
			}
			println! ("{} id        {} num      {} str",
			          s[Id as usize],s[Num as usize],s[Str as usize]);

			for i in 0..N {
				if i<Nrec {
					let i=i as usize;
					let buf=&recs[i*REC_SZ..(i+1)*REC_SZ];

					let r=buf.as_ptr () as *const Record;
					let r = unsafe {
						&*r
					};

					println! ("  {:<8}  {:>8}  {:>6}   {:4}",
					          r.id, r.num, str::from_utf8 (&r.str).unwrap (),
					          knob[i as usize]);
				}
				else {
					println! ("--blank--                       {}",knob[i as usize]);
				}
			}
			println! ();

			(self.data.CS, self.data.NS) = (resp.CS, resp.NS)
		}
		else {
			panic! ("Error in request parameters");
		}
	}
}

fn main () {
	fn print_usage () {
		println! ("{USAGE}");
	}

	fn raw_stdin () {
		use libc::*;

		let mut tio = core::mem::MaybeUninit::uninit ();
		unsafe {
			tcgetattr (STDIN_FILENO, tio.as_mut_ptr ())
		};

		let mut tio = unsafe {
			tio.assume_init ()
		};

		tio.c_lflag &= !(ICANON|ECHO);

		unsafe {
			tcsetattr (STDIN_FILENO, TCSANOW, &tio);
		}
	}
	raw_stdin ();

	let mut clt=Client::new ();
	clt.fire ();

	for c in io::stdin ().bytes () {
		let c=c.unwrap ();

		use Cmd::*;
		match c {
			b'0'..=b'9' => clt.submit (Pos (u32::from (c-b'0'))),

			b'q'|b'w'|b'e' => {
				let a=[b'q', b'w', b'e'];
				for i in 0..Column::NumCol as u32 {
					if c==a[i as usize] {
						clt.submit (Col (i));
						break
					}
				}
			}

			27 => {
				let c=io::stdin ().bytes ().next ().unwrap ().unwrap ();
				assert_eq! (c,b'[');

				let c=io::stdin ().bytes ().next ().unwrap ().unwrap ();
				match c {
					b'A' => clt.submit (OneRow (true)),   // up
					b'B' => clt.submit (OneRow (false)),  // down

					b'5' => if let b'~'=io::stdin ().bytes ().next ().unwrap ().unwrap () {  // PageUp
						clt.submit (OnePage (true))
					}
					b'6' => if let b'~'=io::stdin ().bytes ().next ().unwrap ().unwrap () {  // PageDown
						clt.submit (OnePage (false))
					}

					b'H' => clt.submit (Home (true)) ,  // Home
					b'F' => clt.submit (Home (false)),  // End

					_    => {
						print_usage ();
						continue;
					}
				}
			}

			_ => {
				print_usage ();
				continue;
			}
		}
	}
}
