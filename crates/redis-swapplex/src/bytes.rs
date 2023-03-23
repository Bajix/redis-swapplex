pub trait IntoBytes {
  fn into_bytes(self) -> Vec<u8>;
}

macro_rules! itoa_into_bytes {
  ($t:ty) => {
    impl IntoBytes for $t {
      fn into_bytes(self) -> Vec<u8> {
        let mut buf = ::itoa::Buffer::new();
        let s = buf.format(self);
        s.as_bytes().to_vec()
      }
    }
  };
}

macro_rules! non_zero_itoa_into_bytes {
  ($t:ty) => {
    impl IntoBytes for $t {
      fn into_bytes(self) -> Vec<u8> {
        let mut buf = ::itoa::Buffer::new();
        let s = buf.format(self.get());
        s.as_bytes().to_vec()
      }
    }
  };
}

macro_rules! ryu_into_bytes {
  ($t:ty) => {
    impl IntoBytes for $t {
      fn into_bytes(self) -> Vec<u8> {
        let mut buf = ::ryu::Buffer::new();
        let s = buf.format(self);
        s.as_bytes().to_vec()
      }
    }
  };
}

itoa_into_bytes!(i8);
itoa_into_bytes!(u8);
itoa_into_bytes!(i16);
itoa_into_bytes!(u16);
itoa_into_bytes!(i32);
itoa_into_bytes!(u32);
itoa_into_bytes!(i64);
itoa_into_bytes!(u64);
itoa_into_bytes!(isize);
itoa_into_bytes!(usize);

non_zero_itoa_into_bytes!(core::num::NonZeroU8);
non_zero_itoa_into_bytes!(core::num::NonZeroI8);
non_zero_itoa_into_bytes!(core::num::NonZeroU16);
non_zero_itoa_into_bytes!(core::num::NonZeroI16);
non_zero_itoa_into_bytes!(core::num::NonZeroU32);
non_zero_itoa_into_bytes!(core::num::NonZeroI32);
non_zero_itoa_into_bytes!(core::num::NonZeroU64);
non_zero_itoa_into_bytes!(core::num::NonZeroI64);
non_zero_itoa_into_bytes!(core::num::NonZeroUsize);
non_zero_itoa_into_bytes!(core::num::NonZeroIsize);

ryu_into_bytes!(f32);
ryu_into_bytes!(f64);

impl IntoBytes for bool {
  fn into_bytes(self) -> Vec<u8> {
    if self {
      vec![1]
    } else {
      vec![0]
    }
  }
}

impl IntoBytes for String {
  fn into_bytes(self) -> Vec<u8> {
    self.into_bytes()
  }
}

impl<'a> IntoBytes for &'a str {
  fn into_bytes(self) -> Vec<u8> {
    self.as_bytes().to_vec()
  }
}

impl IntoBytes for Vec<u8> {
  fn into_bytes(self) -> Vec<u8> {
    self
  }
}

impl<'a> IntoBytes for &'a [u8] {
  fn into_bytes(self) -> Vec<u8> {
    self.to_vec()
  }
}
