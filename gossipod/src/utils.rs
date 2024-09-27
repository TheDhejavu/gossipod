use std::fmt::Debug;

pub(crate) fn pretty_debug<T: Debug>(label: &str, value: &T) -> String {
    format!("{} {:#?}", label, value)
}