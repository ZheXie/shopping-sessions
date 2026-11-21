//! Contest CSV dialect: `\n` ends lines; `\r` is data; quotes wrap whole fields only.

/// Parse one line into exactly `N` fields. Empty field → empty slice.
#[inline]
pub fn parse_line_n<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    if line.is_empty() {
        return None;
    }
    let mut fields: [&[u8]; N] = [&[]; N];
    let n = line.len();
    let mut i = 0usize;
    let mut col = 0usize;
    loop {
        if col >= N {
            return None;
        }
        if i < n && line[i] == b'"' {
            let mut j = i + 1;
            while j < n && line[j] != b'"' {
                j += 1;
            }
            if j >= n {
                return None;
            }
            if j + 1 < n && line[j + 1] != b',' {
                return None;
            }
            fields[col] = &line[i + 1..j];
            col += 1;
            i = j + 1;
            if i >= n {
                break;
            }
            if line[i] != b',' {
                return None;
            }
            i += 1;
            continue;
        }
        let mut j = i;
        while j < n && line[j] != b',' {
            if line[j] == b'"' {
                return None;
            }
            j += 1;
        }
        fields[col] = &line[i..j];
        col += 1;
        i = j;
        if i >= n {
            break;
        }
        i += 1;
        if i == n {
            if col >= N {
                return None;
            }
            fields[col] = b"";
            col += 1;
            break;
        }
    }
    if col != N {
        return None;
    }
    Some(fields)
}

/// Split on `\n`, drop final empty from trailing `\n`, skip header.
#[inline]
pub fn data_lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut text = bytes;
    if text.ends_with(b"\n") {
        text = &text[..text.len() - 1];
    }
    let mut it = text.split(|b| *b == b'\n');
    let _header = it.next();
    it
}
