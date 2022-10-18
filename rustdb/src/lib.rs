/* TODO (v2):
	-improve API (add Default, Drop, etc., make impl more safe)
	-make Index generic over an external type (e.g. Record, which also has to be generic).
	-only allocate the necessary number of levels (not always MAXLVL)
	-MAXLVL is a function of current number of elements, increased/shrinked depending on it
	-ncurses client
	-some sort of integration tests
	-todos over text
 */

#![allow (non_snake_case)]

use core::{
	array,
	slice
};
use std::{
	cmp,
	fmt,
	mem,
	ptr,
	str,
	sync::Arc
};
use memoffset::offset_of;
use rand::{Rng,thread_rng};
use rayon::prelude::*;

/* Sample record fields:
	id : u32 - primary key
	num: i32
	s  : &str (represented as [u8;SLEN])
 */
#[derive (Copy,Clone,Debug,PartialEq,PartialOrd)]
pub enum Column { Id, Num, Str, NumCol }

pub const SLEN:usize=4;        // string length of table's s field

#[derive (Debug, Copy,Clone, Default)]
pub struct Record {
	pub id : u32,
	pub num: i32,
	pub str: [u8;SLEN]
}
pub const REC_SZ:usize = mem::size_of::<Record> ();

impl Record {
	pub fn gen (id:u32)->Self {
		Self {
			id,
			num: thread_rng ().gen_range (-1000000..=1000000),
			str: array::from_fn (|_| thread_rng ().gen_range ('a'..='z') as u8)
		}
	}
}

impl fmt::Display for Record {
	fn fmt (&self, f:&mut fmt::Formatter)->fmt::Result {
		let str = str::from_utf8 (&self.str).unwrap ();
		write! (f, "id={}, num={}, str={str}", self.id, self.num)
	}
}

#[derive (Debug, Copy,Clone)]
#[repr (packed)]
pub struct Request {
	pub col: Column ,  // column to sort by
	pub bw : bool   ,  // is sorting backwards?
	pub N  : u32    ,  // screen height
	pub CS : u32    ,  // client's Current Screen
	pub NS : u32       // Number of Screens (from client's perspective)
}
pub const REQ_SZ:usize = mem::size_of::<Request> ();

#[derive (Debug, Copy,Clone)]
#[repr (packed)]
pub struct Response {
	pub ok: bool,      // request status
	pub CS: u32 ,      // Current Screen that is being passed to client
	pub NS: u32        // Number of Screens (server's version)
}
pub const RESP_SZ:usize = mem::size_of::<Response> ();

#[derive (Debug)]
enum Dt {U32, I32, STR}  // possible data types for table columns

/* A table consists of:
 - arbitrary number of records with columns of defined data types
 - the index for each column in the form of skip lists
 */
#[derive (Debug)]
pub struct Table {
	fields: [Index; Column::NumCol as usize],
	tot   : u32
}

impl Table {
	pub fn new ()->Self {
		Table {
			fields: [
				// TODO: A lot of code repeat here, need to make it generic over Record type
				Index::new (offset_of! (Record,id ), Dt::U32),
				Index::new (offset_of! (Record,num), Dt::I32),
				Index::new (offset_of! (Record,str), Dt::STR)
			],
			tot   : 0
		}
	}

	pub fn add (&mut self, rec:Record)->Result<(),()> {
		// id is a primary key, has to be unique
		if self.fields[Column::Id as usize].search (&rec).is_some () {
			return Err (())
		}

		// Record needs to be boxed and freed only when all indexes deleted
		let arc=Arc::new (rec);
		self.fields.par_iter_mut ().for_each (|x| {
			x.insert (arc.clone ());
		});

		self.tot += 1;

		Ok (())
	}

	pub fn rm (&mut self, id:u32)->Result<(),()> {  // we remove only by the primary key
		let rec=self.fields[Column::Id as usize]
		       .delete (&Record {id,..Default::default ()},false).ok_or (())?;

		self.fields.par_iter_mut ().skip (1).for_each (|x| {
			// Delete from other columns, using received Record's values (with the same address)
			x.delete (&rec, true)
			 .expect ("Other index returned none, whilst it shouldn't");

			// The contained Arc value is gonna be dropped, and the count decremented
		});

		self.tot -= 1;

		Ok (())
	}

	pub fn upd (&mut self, new:Record)->Result<(),()> {
		// TODO: do search instead of rm+upd to save (rm+add)'ing of id field
		//   (make search return mutable ref to the Record)
		self.rm (new.id)?;
		self.add (new)?;

		Ok (())
	}

	pub fn fetch (&self, req:Request)->(Response,Vec<u8>) {
		let mut resp=Response {
			ok: false,
			CS: 0,
			NS: 1
		};
		let mut data=Vec::new ();

		if req.col>=Column::NumCol || req.N==0 || req.NS==0 || req.CS>=req.NS {
			return (resp,data)
		}
		resp.ok=true;

		if self.tot==0 {
			return (resp,data)
		}

		/* Fill response */
		let mut pos = if req.NS>1 && self.tot>req.N {
			(1.0 + req.CS as f32 * (self.tot-req.N) as f32
			                     / (req.NS-1) as f32)
			.round () as u32
		}
		else {
			1
		};
		resp.CS=pos-1;

		if self.tot>req.N {
			resp.NS = self.tot-req.N+1;
		}

		/* Fill data vector */
		if req.bw {
			pos = self.tot-pos + 1
		}
		assert! (pos!=0 && pos<=self.tot);

		let min=cmp::min (req.N, self.tot);                      // if total<req, send total
		let mut cur=self.fields[req.col as usize].lookup (pos);

		for _ in 0..min {
			let r = &*cur.elem as *const Record as *const u8;
			let sl=unsafe {
				slice::from_raw_parts (r,REC_SZ)
			}.to_vec ();

			data.extend (sl);

			cur = unsafe { &*(if req.bw {cur.prev.0} else {cur.next[0].0}) };
		}

		(resp,data)
	}

	pub fn tot (&self)->u32 {
		self.tot
	}
}

// TODO: impl Drop for Table

/* An Index (Skip-list) with fingers and double-link at the 0th level */
const MAXLVL:usize = 24;   // absolute maximum level of a skip-list (i.e. totalmax = 25)
const P     :f32   = 0.5;  // probability of the node propagation to the next level

#[derive (Debug)]
struct Node {
	elem: Arc<Record>,
	next: Vec<Link>,
	fing: Vec<u32>,
	prev: Link
}

#[derive(Debug, Clone)]
struct Link (*mut Node);
unsafe impl Sync for Link {}
// TODO: impl maybe a Deref trait to get rid of ugly .0 dereferences

#[derive (Debug)]
struct Index {
	head: Link ,
	l   : usize,  // total #levels (inc.0)
	off : usize,  // field offset from the start
	dt  : Dt      // datatype of the field
}
unsafe impl Send for Index {}

impl Index {
	fn new (off:usize, dt:Dt)->Self {
		Self {
			head: Link (Box::into_raw (Box::new (Node {
				elem: Default::default (),
				next: vec![Link (ptr::null_mut ()); MAXLVL+1],
				fing: vec![0u32                   ; MAXLVL+1],
				prev: Link (ptr::null_mut ())
			}))),
			l   : 0,
			off,
			dt
		}
	}

	fn comp (&self, a:&Record, b:&Record)->cmp::Ordering {
		let a=a as *const Record as *const u8;
		let b=b as *const Record as *const u8;

		unsafe {
			match self.dt {
				Dt::U32=> {
					let a= *(a.add (self.off) as *const u32);
					let b= *(b.add (self.off) as *const u32);
					a.cmp (&b)
				}
				Dt::I32=> {
					let a= *(a.add (self.off) as *const i32);
					let b= *(b.add (self.off) as *const i32);
					a.cmp (&b)
				}
				Dt::STR=> {
					let a=str::from_utf8 (&slice::from_raw_parts (a.add (self.off),SLEN)).unwrap ();
					let b=str::from_utf8 (&slice::from_raw_parts (b.add (self.off),SLEN)).unwrap ();
					a.cmp (&b)
				}
			}
		}
	}

	fn search (&self, elem:&Record)->Option<&Node> {
		let mut cur = self.head.0;

		unsafe {
			for l in (0..self.l).rev () {
				while !(*cur).next[l].0.is_null () && self.comp (elem, &(*(*cur).next[l].0).elem).is_gt () {
					cur = (*cur).next[l].0;
				}
			}

			if !(*cur).next[0].0.is_null () && self.comp (elem, &(*(*cur).next[0].0).elem).is_eq () {
				Some (&(*(*cur).next[0].0))
			}
			else {
				None
			}
		}
	}

	fn lookup (&self, n:u32)->&Node {
		let mut cur = self.head.0;
		let mut s:u32=0;

		unsafe {
			for l in (0..self.l).rev () {
				while !(*cur).next[l].0.is_null () && n > s+(*cur).fing[l] {
					s += (*cur).fing[l];
					cur = (*cur).next[l].0;
				}
			}
			assert! (!(*cur).next[0].0.is_null ());

			&(*(*cur).next[0].0)
		}
	}

	fn insert (&mut self, elem:Arc<Record>) {
		let node = Box::into_raw (Box::new (Node {
			elem,
			next: vec![Link (ptr::null_mut ()); MAXLVL+1],
			fing: vec![0u32                   ; MAXLVL+1],
			prev: Link (ptr::null_mut ())
		}));

		let mut cur = self.head.0;
		let mut prv = vec![cur;  MAXLVL+1];  // bread-crumbs of our visit per level
		let mut d   = vec![0u32; MAXLVL+1];  // distances from the previous nodes per level
		let mut f   = vec![0u32; MAXLVL+1];  // fingers per level

		// Find a place where to insert
		for l in (0..self.l).rev () {
			unsafe {
				while !(*cur).next[l].0.is_null () && self.comp (&(*node).elem, &(*(*cur).next[l].0).elem).is_gt () {
					d[l] += (*cur).fing[l];
					cur = (*cur).next[l].0;
				}
			}
			prv[l] = cur;
		}

		// Randomly determine the maxlevel of the current node
		let rval=thread_rng ().gen_range (0..=LIMS[MAXLVL]);
		let maxlvl=get_maxlvl (rval);
		if self.l <= maxlvl {          // maxlvl can be [0..MAXLVL]
			self.l = maxlvl+1;         // total number of levels (can be [1..MAXLVL+1])
		}

		// Update fingers
		for l in 1..self.l {
			for k in 0..l {
				f[l] += d[k];
			}
		}

		// Insert element
		unsafe {
			for l in 0..self.l {
				if l<=maxlvl {
					(*node).next[l].0 = (*prv[l]).next[l].0;
					(*prv[l]).next[l].0 = node;

					// when maxlvl is new highest wrap-arounds are possible
					(*node).fing[l] = (*prv[l]).fing[l].wrapping_sub (f[l]);
					(*prv[l]).fing[l] = f[l]+1;
				}
				else {
					(*prv[l]).fing[l] = (*prv[l]).fing[l].wrapping_add (1);
				}
			}
			if !((*node).next[0].0).is_null () {
				(*(*node).next[0].0).prev.0 = node;
			}
			(*node).prev.0 = prv[0];
		}
	}

	fn delete (&mut self, elem:&Record, same:bool)->Option<Arc<Record>> {
		let mut cur = self.head.0;
		let mut prv = vec![cur; MAXLVL+1];  // bread-crumbs of our visit per level

		for l in (0..self.l).rev () {
			unsafe {
				while !(*cur).next[l].0.is_null () && self.comp (elem, &(*(*cur).next[l].0).elem).is_gt () {
					cur = (*cur).next[l].0;
				}
			}
			prv[l] = cur;
		}

		unsafe {
			if !(*cur).next[0].0.is_null () && self.comp (elem, &(*(*cur).next[0].0).elem).is_eq () {
				if same {
					while {
						let a1=&*(*(*cur).next[0].0).elem as *const Record;
						let a2=elem as *const Record;
						a1 != a2
					} {
						cur = (*cur).next[0].0;
						assert! (!cur.is_null ());

						for l in (0..self.l).rev () {
							if (*prv[l]).next[l].0==cur {
								prv[l] = cur;
							}
						}
					}
				}

				let tmp = (*cur).next[0].0;
				for l in 0..self.l {
					if (*prv[l]).next[l].0==tmp {
						(*prv[l]).next[l].0 = (*tmp).next[l].0;
					}
					(*prv[l]).fing[l] = (*prv[l]).fing[l]
					                  .wrapping_add ((*tmp).fing[l].wrapping_sub (1));

					if (*self.head.0).next[l].0.is_null () {
						self.l -= 1;
					}
				}
				if !(*tmp).next[0].0.is_null () {
					(*(*tmp).next[0].0).prev.0 = (*tmp).prev.0;
				}

				let b=Box::from_raw (tmp);
				Some (b.elem)
			}
			else {
				None
			}
		}
	}
}

// TODO: impl Drop for Index

const LIMS: [u32;MAXLVL+1] = {
	let mut lims = [0;MAXLVL+1];
	let mut i=1;
	while i<=MAXLVL {
		lims[i] = lims[i-1] * (1.0/P) as u32 + 1;
		i += 1;
	}
	lims
};

// Determine the maxlvl for an element being inserted
fn get_maxlvl (rval:u32)->usize {
	for (i,item) in LIMS.iter ().enumerate () {
		if rval <= *item {
			return MAXLVL-i
		}
	}
	unreachable! ();
}


#[cfg (test)]
mod tests {
	use super::*;

	fn print_off (dt:&Dt, &off:&usize, r:&Record)->String {
		let r=r as *const Record as *const u8;
		unsafe {
			match dt {
				Dt::U32=> {
					let r= *(r.add (off) as *const u32);
					format! ("{}", r)
				}
				Dt::I32=> {
					let r= *(r.add (off) as *const i32);
					format! ("{}", r)
				}
				Dt::STR=> {
					let r=str::from_utf8 (&slice::from_raw_parts (r.add (off),SLEN)).unwrap ();
					format! ("{}", r)
				}
			}
		}
	}

	#[test]
	fn check_lims () {
		assert_eq! (LIMS, [0,1,3,7,15,31,63,127,255,511,1023,2047,4095,8191,16383,32767,65535,131071,262143,524287,1048575,2097151,4194303,8388607,16777215]);
	}

	#[test]
	fn maxlvl () {
		assert_eq! (24,get_maxlvl (0));
		assert_eq! (23,get_maxlvl (1));
		assert_eq! (22,get_maxlvl (2));
		assert_eq! (22,get_maxlvl (3));
		assert_eq! (21,get_maxlvl (4));
		assert_eq! (0,get_maxlvl (16777210));
	}

	#[test]
	fn off () {
		let i=offset_of! (Record,id);
		let n=offset_of! (Record,num);
		let s=offset_of! (Record,str);
		assert_eq! (i,0);
		assert_eq! (n,4);
		assert_eq! (s,8);
	}

	impl Index {
		#[allow (dead_code)]
		fn print (&self) {
			println! ("Index = {self}");
			unsafe {
				let a=(*self.head.0).next[0].0;
				if a.is_null () {
					println! ("null");
				}
				else {
					let b=&(*a).elem;
					println! ("count={}", Arc::strong_count (b));
				}
			}
		}
	}
	impl fmt::Display for Index {
		fn fmt (&self, f:&mut fmt::Formatter)->fmt::Result {
			write! (f, "[")?;

			let mut cur=self.head.0;
			unsafe {
				while !(*cur).next[0].0.is_null () {
					if cur!=self.head.0 {
						write! (f, ",")?;
					}

					let el=print_off (&self.dt, &self.off, &(*(*cur).next[0].0).elem);
					write! (f,"{el}")?;

					cur = (*cur).next[0].0;
				}
			}

			write! (f, "]")
		}
	}

	#[test]
	fn basic_index () {
		// New
		let mut sl=Index::new (offset_of! (Record,id), Dt::U32);
		assert_eq! (format! ("{sl}"), "[]");
		// sl.print ();

		// Insert
		sl.insert (Arc::new (Record { id: 50, ..Default::default () }));
		assert_eq! (format! ("{sl}"), "[50]");
		sl.insert (Arc::new (Record { id: 130, ..Default::default () }));
		assert_eq! (format! ("{sl}"), "[50,130]");

		let nums=[80,150,90,40,170,20,35,642,46442,454,23,4,35,3];
		for id in nums {
			sl.insert (Arc::new (Record { id, ..Default::default () }));
		}
		assert_eq! (format! ("{sl}"), "[3,4,20,23,35,35,40,50,80,90,130,150,170,454,642,46442]");

		// Search
		for (n,res) in [
			( 80, true ),
			(150, true ),
			( 90, true ),
			( 40, true ),
			(170, true ),
			( 20, true ),
			( 12, false),
			( 64, false),
			( 24, false),
			(  0, false)
		] {
			let el=sl.search (&Record { id: n, ..Default::default () });
			assert_eq! (el.is_some (), res);
		}

		// Lookup
		let a=[3,4,20,23,35,35,40,50,80,90,130,150,170,454,642,46442];
		for n in 0..a.len () {
			let el=(*sl.lookup (n as u32+1).elem).id;
			assert_eq! (el, a[n]);
		}

		// Delete
		for (id,res) in [
			(   80, true ),
			(  150, true ),
			(   90, true ),
			(   40, true ),
			(  170, true ),
			(   20, true ),
			(   35, true ),
			(  642, true ),
			(46442, true ),
			(  454, true ),
			(   23, true ),
			(    4, true ),
			(   35, true ),
			(    3, true ),
			(46442, false),
			(  454, false),
			(   23, false),
			(    4, false),
			(   35, false),
			(    3, false),
			(   50, true ),
			(  130, true ),
			(    0, false)
		] {
			let el=sl.delete (&Record { id, ..Default::default () }, false);
			assert_eq! (el.is_some (), res);
		}
	}

	#[test]
	fn basic_table () {
		let mut t=Table::new ();

		let id=0;
		assert! (t.add (Record::gen (id)).is_ok ());
		assert! (t.upd (Record::gen (id)).is_ok ());

		assert! (t.add (Record::gen (id)).is_err ());
		assert! (t.rm (id).is_ok ());

		assert! (t.rm (id).is_err ());
		assert! (t.upd (Record::gen (id)).is_err ());
	}
}
