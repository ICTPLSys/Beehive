pub use paste::paste;

macro_rules! define_bit {
    ($field:ident, $bit:expr) => {
        #[inline(always)]
        pub fn $field(&self) -> bool {
            self.0 & (1 << $bit) != 0
        }

        paste! {
            #[inline(always)]
            pub fn [<set_ $field>](&mut self, value: bool) {
                self.0 = (self.0 & !(1 << $bit)) | ((value as u64) << $bit);
            }

            #[inline(always)]
            const fn [<init_ $field>](value: bool) -> u64 {
                (value as u64) << $bit
            }
        }
    };
}

macro_rules! define_bits {
    ($field:ident, $lsb:expr, $msb:expr) => {
        #[inline(always)]
        pub fn $field(&self) -> u64 {
            let width = $msb - $lsb + 1;
            let mask = (1 << width) - 1;
            (self.0 >> $lsb) & mask
        }

        paste! {
            #[inline(always)]
            pub fn [<set_ $field>](&mut self, value: u64) {
                let width = $msb - $lsb + 1;
                let mask = (1 << width) - 1;
                debug_assert!(value <= mask);
                self.0 = (self.0 & !(mask << $lsb)) | (value << $lsb);
            }

            #[inline(always)]
            const fn [<init_ $field>](value: u64) -> u64 {
                let width = $msb - $lsb + 1;
                let mask = (1 << width) - 1;
                debug_assert!(value <= mask);
                (value as u64) << $lsb
            }
        }
    };
}
