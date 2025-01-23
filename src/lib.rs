use std::borrow::Cow;

mod buffer;
mod encodex;
mod file;
mod log;
mod page;

pub(crate) fn assert_borrowed_type<T: ?Sized + ToOwned>(value: Cow<T>) -> &T {
    match value {
        Cow::Borrowed(reference) => reference,
        Cow::Owned(_) => panic!("Cow::Owned was not borrowed"),
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_borrowed_type;
    use std::borrow::Cow;

    #[test]
    fn assert_borrowed_type_and_get_the_reference() {
        let value: Cow<'_, str> = Cow::Borrowed("LSM-based storage engine");
        let reference = assert_borrowed_type(value);
        assert_eq!("LSM-based storage engine", reference);
    }

    #[test]
    #[should_panic]
    fn assert_borrowed_type_fails() {
        let value: Cow<'_, String> = Cow::Owned(String::from("Raft"));
        assert_borrowed_type(value);
    }
}
