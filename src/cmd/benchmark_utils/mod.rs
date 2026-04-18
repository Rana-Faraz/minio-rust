use rand::Rng;

pub fn get_random_byte() -> u8 {
    const LETTER_BYTES: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let index = rand::thread_rng().gen_range(0..LETTER_BYTES.len());
    LETTER_BYTES[index]
}

pub fn generate_bytes_data(size: usize) -> Vec<u8> {
    vec![get_random_byte(); size]
}
