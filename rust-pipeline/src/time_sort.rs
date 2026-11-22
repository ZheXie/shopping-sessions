//! Sorting by timestamp only. Payloads at equal times stay in the same session.
use std::arch::x86_64::*;

/// Three 11-bit digits cover the usual event/click window. The caller checks its
/// actual range first and supplies the scatter workspace from person grouping.
pub fn radix_time<T: Copy>(values: &mut [T], scratch: &mut [T], time: impl Fn(T) -> u64) {
    const MASK: usize = 2047;
    let mut counts = [[0u32; MASK + 1]; 3];
    for &v in values.iter() {
        let t = time(v) as usize;
        counts[0][t & MASK] += 1;
        counts[1][(t >> 11) & MASK] += 1;
        counts[2][(t >> 22) & MASK] += 1;
    }
    for digit in &mut counts {
        let mut sum = 0;
        for pos in digit {
            let count = *pos;
            *pos = sum;
            sum += count;
        }
    }
    let mut src = values.as_mut_ptr();
    let mut dst = scratch.as_mut_ptr();
    assert!(scratch.len() >= values.len());
    for (pass, positions) in counts.iter_mut().enumerate() {
        for i in 0..values.len() {
            unsafe {
                let v = *src.add(i);
                let pos = &mut positions[(time(v) as usize >> (pass * 11)) & MASK];
                dst.add(*pos as usize).write(v);
                *pos += 1;
            }
        }
        std::mem::swap(&mut src, &mut dst);
    }
    values.copy_from_slice(&scratch[..values.len()]);
}

// Each layer compares every lane to the lane one, two or four positions
// away. The mask selects which lanes take the larger member of their pair.
#[inline(always)]
unsafe fn adjacent<const MAX: u8>(v: __m512i) -> __m512i {
    let neighbor = _mm512_permutex_epi64::<0xb1>(v);
    _mm512_mask_max_epu64(_mm512_min_epu64(v, neighbor), MAX, v, neighbor)
}

#[inline(always)]
unsafe fn pairs<const MAX: u8>(v: __m512i) -> __m512i {
    let neighbor = _mm512_permutex_epi64::<0x4e>(v);
    _mm512_mask_max_epu64(_mm512_min_epu64(v, neighbor), MAX, v, neighbor)
}

#[inline(always)]
unsafe fn halves<const MAX: u8>(v: __m512i) -> __m512i {
    let neighbor = _mm512_shuffle_i64x2::<0x4e>(v, v);
    _mm512_mask_max_epu64(_mm512_min_epu64(v, neighbor), MAX, v, neighbor)
}

#[inline(always)]
unsafe fn finish_bitonic(v: __m512i) -> __m512i {
    adjacent::<0xaa>(pairs::<0xcc>(halves::<0xf0>(v)))
}

#[inline(always)]
unsafe fn sort_eight(v: __m512i) -> __m512i {
    finish_bitonic(adjacent::<0x5a>(pairs::<0x3c>(adjacent::<0x66>(v))))
}

#[inline(always)]
unsafe fn reverse(v: __m512i) -> __m512i {
    _mm512_permutexvar_epi64(_mm512_set_epi64(0, 1, 2, 3, 4, 5, 6, 7), v)
}

/// Merge two ascending runs in N registers. The first stage compares
/// opposite ends, then each half is bitonic and can be merged independently.
#[inline(always)]
unsafe fn merge<const N: usize>(regs: &mut [__m512i]) {
    for i in 0..N / 2 {
        let j = N - 1 - i;
        let a = regs[i];
        let b = reverse(regs[j]);
        regs[i] = _mm512_min_epu64(a, b);
        regs[j] = reverse(_mm512_max_epu64(a, b));
    }
    let mut stride = N / 4;
    while stride != 0 {
        for pair in regs.chunks_exact_mut(2 * stride) {
            for i in 0..stride {
                let (a, b) = (pair[i], pair[i + stride]);
                pair[i] = _mm512_min_epu64(a, b);
                pair[i + stride] = _mm512_max_epu64(a, b);
            }
        }
        stride /= 2;
    }
    for reg in regs {
        *reg = finish_bitonic(*reg);
    }
}

#[inline(always)]
unsafe fn network<const N: usize>(values: &mut [u64]) {
    let sentinel = _mm512_set1_epi64(-1);
    let mut regs = [sentinel; N];
    let p = values.as_mut_ptr();
    let full = values.len() / 8;
    let tail = values.len() % 8;
    let mask = ((1u16 << tail) - 1) as u8;
    for i in 0..full {
        regs[i] = sort_eight(_mm512_loadu_si512(p.add(i * 8).cast()));
    }
    if tail != 0 {
        regs[full] = sort_eight(_mm512_mask_loadu_epi64(
            sentinel,
            mask,
            p.add(full * 8).cast(),
        ));
    }
    for run in regs.chunks_exact_mut(2) {
        merge::<2>(run);
    }
    for run in regs.chunks_exact_mut(4) {
        merge::<4>(run);
    }
    for run in regs.chunks_exact_mut(8) {
        merge::<8>(run);
    }
    for run in regs.chunks_exact_mut(16) {
        merge::<16>(run);
    }
    for i in 0..full {
        _mm512_storeu_si512(p.add(i * 8).cast(), regs[i]);
    }
    if tail != 0 {
        _mm512_mask_storeu_epi64(p.add(full * 8).cast(), mask, regs[full]);
    }
}

/// Caller has checked AVX-512F once per process, and len <= 128.
#[target_feature(enable = "avx512f")]
pub unsafe fn small(values: &mut [u64]) {
    match values.len() {
        0 | 1 => (),
        2..=8 => network::<1>(values),
        9..=16 => network::<2>(values),
        17..=32 => network::<4>(values),
        33..=64 => network::<8>(values),
        65..=128 => network::<16>(values),
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn random(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    #[test]
    fn network_matches_full_sort_at_every_tail_length() {
        if !is_x86_feature_detected!("avx512f") {
            return;
        }
        let mut state = 1;
        for len in 0..=128 {
            for _ in 0..20 {
                let mut values: Vec<_> = (0..len)
                    .map(|i| match i % 7 {
                        0 => 0,
                        1 => u64::MAX,
                        _ => random(&mut state),
                    })
                    .collect();
                let mut expected = values.clone();
                expected.sort_unstable();
                unsafe {
                    small(&mut values);
                }
                assert_eq!(values, expected);
            }
        }
    }

    #[test]
    fn radix_moves_the_whole_click_and_preserves_ties() {
        let mut state = 1;
        let mut values: Vec<_> = (0..4096)
            .map(|id| {
                let t = if id % 3 == 0 {
                    1 << 32
                } else {
                    random(&mut state) & ((1 << 33) - 1)
                };
                (t, id)
            })
            .collect();
        let mut expected = values.clone();
        expected.sort_by_key(|c| c.0);
        let mut scratch = vec![(0, 0); values.len()];
        radix_time(&mut values, &mut scratch, |c| c.0);
        assert_eq!(values, expected);
    }
}
