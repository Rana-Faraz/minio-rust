pub fn split_path(path: &str, second_index: bool) -> (String, String) {
    let index = if second_index {
        find_second_index(path, '/')
    } else {
        path.find('/')
    };

    match index {
        Some(index) => (path[..index + 1].to_string(), path[index + 1..].to_string()),
        None => (path.to_string(), String::new()),
    }
}

fn find_second_index(input: &str, needle: char) -> Option<usize> {
    let mut seen_first = false;
    for (index, ch) in input.char_indices() {
        if ch == needle {
            if seen_first {
                return Some(index);
            }
            seen_first = true;
        }
    }
    None
}
