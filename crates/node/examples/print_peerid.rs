//! Print a deterministic libp2p PeerId for a 32-byte hex seed.
fn main() {
    let arg = std::env::args()
        .nth(1)
        .expect("usage: print_peerid <hex seed>");
    let bytes = hex::decode(&arg).expect("hex seed");
    let secret = libp2p::identity::ed25519::SecretKey::try_from_bytes(bytes).expect("secret");
    let keypair = libp2p::identity::ed25519::Keypair::from(secret);
    let kp = libp2p::identity::Keypair::from(keypair);
    println!("{}", kp.public().to_peer_id());
}
