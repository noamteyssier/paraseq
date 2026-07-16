use std::io;
use std::mem::MaybeUninit;

/// Reads from `reader` into `buffer`'s tail, growing (but not
/// zero-initializing) capacity as needed so that `buffer.len()` can reach
/// `target_len`. Never exposes more than `target_len - buffer.len()` bytes to
/// a single `read` call, even if more capacity happens to already be spare
/// (e.g. left over from a previous batch's overshoot).
///
/// This avoids the zero-initialization that `Vec::resize(.., 0)` would
/// otherwise perform on every grow, since the freshly read bytes overwrite it
/// immediately anyway.
pub(crate) fn read_into_uninit<R: io::Read>(
    buffer: &mut Vec<u8>,
    reader: &mut R,
    target_len: usize,
) -> io::Result<usize> {
    let old_len = buffer.len();
    debug_assert!(target_len >= old_len);
    if buffer.capacity() < target_len {
        buffer.reserve(target_len - old_len);
    }
    let spare = &mut buffer.spare_capacity_mut()[..target_len - old_len];

    // SAFETY: `spare` is a `&mut [u8]` view over uninitialized memory,
    // which is safe to hand to `read` as long as we only commit (`set_len`)
    // the prefix that `read` reports as written.
    let spare: &mut [u8] = unsafe { &mut *(spare as *mut [MaybeUninit<u8>] as *mut [u8]) };
    let n = reader.read(spare)?;

    // SAFETY: `read` just reported `n` bytes written into the first `n` bytes
    // of `spare`, which is exactly `buffer`'s tail starting at `old_len`.
    unsafe { buffer.set_len(old_len + n) };
    Ok(n)
}
