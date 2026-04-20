/// Computes `(lo,hi) = a+b+carry` where `hi` is the carry.
#[inline(always)]
pub const fn const_adc(a: u64, b: u64, carry: u64) -> (u64, u64) {
    let (s, c1) = a.overflowing_add(b);
    let (s, c2) = s.overflowing_add(carry);
    (s, (c1 | c2) as u64)
}

/// Computes `a := a+b+carry` and returns the carry.
#[inline(always)]
pub fn adc(a: &mut u64, b: u64, carry: u8) -> u8 {
    #[cfg(target_arch = "x86_64")]
    {
        core::arch::x86_64::_addcarry_u64(carry, *a, b, a)
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        let tmp = (*a as u128) + (b as u128) + (carry as u128);
        *a = tmp as u64;
        (tmp >> 64) as u8
    }
}

/// Returns `a+carry` and updates the carry.
#[inline(always)]
pub fn acc(a: u64, carry: &mut u64) -> u64 {
    let t = a as u128 + *carry as u128;
    *carry = (t >> 64) as u64;
    t as u64
}

/// Computes `(lo,hi) = a - b - borrow` where `hi` is the borrow.
#[inline(always)]
pub const fn const_sbb(a: u64, b: u64, borrow: u64) -> (u64, u64) {
    let (d, b1) = a.overflowing_sub(b);
    let (d, b2) = d.overflowing_sub(borrow);
    (d, (b1 | b2) as u64)
}

/// Computes `a := a - b - borrow` and returns the borrow.
#[inline(always)]
pub fn sbb(a: &mut u64, b: u64, borrow: u8) -> u8 {
    #[cfg(target_arch = "x86_64")]
    {
        core::arch::x86_64::_subborrow_u64(borrow, *a, b, a)
    }

    #[cfg(not(target_arch = "x86_64"))]
    {
        let tmp = (1u128 << 64) + (*a as u128) - (b as u128) - (borrow as u128);
        *a = tmp as u64;
        u8::from(tmp >> 64 == 0)
    }
}

/// Computes `(lo,hi) = a * b + c + carry`.
#[inline(always)]
pub const fn const_mac(a: u64, b: u64, c: u64, carry: u64) -> (u64, u64) {
    let tmp = a as u128 * b as u128 + c as u128 + carry as u128;
    (tmp as u64, (tmp >> 64) as u64)
}

/// Computes `(lo,hi) = a * b + c + carry`.
#[inline(always)]
pub fn mac(a: u64, b: u64, c: u64, carry: &mut u64) -> u64 {
    let tmp = a as u128 * b as u128 + c as u128 + *carry as u128;
    *carry = (tmp >> 64) as u64;
    tmp as u64
}
