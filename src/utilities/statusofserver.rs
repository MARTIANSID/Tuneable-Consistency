#[derive(PartialEq)]
pub enum StatusOfServer {
    LEADER,
    CANDIDATE,
    FOLLOWER,
}
 