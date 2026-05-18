//! Contains constructs for describing the nodes in a Binary Merkle Patricia
//! Tree used by Starknet.
//!
//! For more information about how these Starknet trees are structured, see
//! [`MerkleTree`](crate::tree::MerkleTree).

use std::cell::RefCell;
use std::rc::Rc;

use bitvec::order::Msb0;
use bitvec::prelude::BitVec;
use bitvec::slice::BitSlice;
use pathfinder_common::hash::FeltHash;
use pathfinder_crypto::Felt;
use pathfinder_storage::TrieStorageIndex;

/// A node in a Binary Merkle-Patricia Tree graph.
#[derive(Clone, Debug, PartialEq)]
pub enum InternalNode {
    /// A node that has not been fetched from storage yet.
    ///
    /// As such, all we know is its index.
    Unresolved(TrieStorageIndex),
    /// A branch node with exactly two children.
    Binary(BinaryNode),
    /// Describes a path connecting two other nodes.
    Edge(EdgeNode),
    /// A leaf node.
    Leaf,
}

/// Describes the [InternalNode::Binary] variant.
#[derive(Clone, Debug, PartialEq)]
pub struct BinaryNode {
    /// The storage index of this node (if it was loaded from storage).
    pub storage_index: Option<TrieStorageIndex>,
    /// The height of this node in the tree.
    pub height: usize,
    /// [Left](Direction::Left) child.
    pub left: Rc<RefCell<InternalNode>>,
    /// [Right](Direction::Right) child.
    pub right: Rc<RefCell<InternalNode>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct EdgeNode {
    /// The storage index of this node (if it was loaded from storage).
    pub storage_index: Option<TrieStorageIndex>,
    /// The starting height of this node in the tree.
    pub height: usize,
    /// The path this edge takes.
    pub path: BitVec<u8, Msb0>,
    /// The child of this node.
    pub child: Rc<RefCell<InternalNode>>,
}

/// Describes the direction a child of a [BinaryNode] may have.
///
/// Binary nodes have two children, one left and one right.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Left,
    Right,
}

impl Direction {
    /// Inverts the [Direction].
    ///
    /// [Left] becomes [Right], and [Right] becomes [Left].
    ///
    /// [Left]: Direction::Left
    /// [Right]: Direction::Right
    pub fn invert(self) -> Direction {
        match self {
            Direction::Left => Direction::Right,
            Direction::Right => Direction::Left,
        }
    }
}

impl From<bool> for Direction {
    fn from(tf: bool) -> Self {
        match tf {
            true => Direction::Right,
            false => Direction::Left,
        }
    }
}

impl From<Direction> for bool {
    fn from(direction: Direction) -> Self {
        match direction {
            Direction::Left => false,
            Direction::Right => true,
        }
    }
}

impl BinaryNode {
    /// Maps the key's bit at the binary node's height to a [Direction].
    ///
    /// This can be used to check which direction the key describes in the
    /// context of this binary node i.e. which direction the child along the
    /// key's path would take.
    pub fn direction(&self, key: &BitSlice<u8, Msb0>) -> Direction {
        key[self.height].into()
    }

    /// Returns the [Left] or [Right] child.
    ///
    /// [Left]: Direction::Left
    /// [Right]: Direction::Right
    pub fn get_child(&self, direction: Direction) -> Rc<RefCell<InternalNode>> {
        match direction {
            Direction::Left => self.left.clone(),
            Direction::Right => self.right.clone(),
        }
    }

    pub(crate) fn calculate_hash<H: FeltHash>(left: Felt, right: Felt) -> Felt {
        H::hash(left, right)
    }
}

impl InternalNode {
    pub fn is_binary(&self) -> bool {
        matches!(self, InternalNode::Binary(..))
    }

    pub fn as_binary(&self) -> Option<&BinaryNode> {
        match self {
            InternalNode::Binary(binary) => Some(binary),
            _ => None,
        }
    }

    pub fn as_edge(&self) -> Option<&EdgeNode> {
        match self {
            InternalNode::Edge(edge) => Some(edge),
            _ => None,
        }
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self, InternalNode::Leaf)
    }

    pub fn storage_index(&self) -> Option<TrieStorageIndex> {
        match self {
            InternalNode::Unresolved(storage_index) => Some(*storage_index),
            InternalNode::Binary(binary) => binary.storage_index,
            InternalNode::Edge(edge) => edge.storage_index,
            InternalNode::Leaf => None,
        }
    }
}

impl EdgeNode {
    /// Returns true if the edge node's path matches the same path given by the
    /// key.
    pub fn path_matches(&self, key: &BitSlice<u8, Msb0>) -> bool {
        self.path == key[self.height..self.height + self.path.len()]
    }

    /// Returns the common bit prefix between the edge node's path and the given
    /// key.
    ///
    /// This is calculated with the edge's height taken into account.
    pub fn common_path(&self, key: &BitSlice<u8, Msb0>) -> &BitSlice<u8, Msb0> {
        let key_path = key.iter().skip(self.height);
        let common_length = key_path
            .zip(self.path.iter())
            .take_while(|(a, b)| a == b)
            .count();

        &self.path[..common_length]
    }

    pub(crate) fn calculate_hash<H: FeltHash>(child: Felt, path: &BitSlice<u8, Msb0>) -> Felt {
        let mut length = [0; 32];
        // Safe as len() is guaranteed to be <= 251
        length[31] = path.len() as u8;
        let length = Felt::from_be_bytes(length).unwrap();
        let path = Felt::from_bits(path).unwrap();

        H::hash(child, path) + length
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_common::hash::PedersenHash;

    use super::*;

    mod direction {
        use Direction::*;

        use super::*;

        #[test]
        fn invert() {
            assert_eq!(Left.invert(), Right);
            assert_eq!(Right.invert(), Left);
        }

        #[test]
        fn bool_round_trip() {
            assert_eq!(Direction::from(bool::from(Left)), Left);
            assert_eq!(Direction::from(bool::from(Right)), Right);
        }

        #[test]
        fn right_is_true() {
            assert!(bool::from(Right));
        }

        #[test]
        fn left_is_false() {
            assert!(!bool::from(Left));
        }
    }

    mod binary {
        use bitvec::bitvec;
        use pathfinder_common::felt;

        use super::*;

        #[test]
        fn direction() {
            let uut = BinaryNode {
                storage_index: None,
                height: 1,
                left: Rc::new(RefCell::new(InternalNode::Unresolved(
                    TrieStorageIndex::new(1).unwrap(),
                ))),
                right: Rc::new(RefCell::new(InternalNode::Unresolved(
                    TrieStorageIndex::new(2).unwrap(),
                ))),
            };

            let mut zero_key = bitvec![u8, Msb0; 1; 251];
            zero_key.set(1, false);

            let mut one_key = bitvec![u8, Msb0; 0; 251];
            one_key.set(1, true);

            let zero_direction = uut.direction(&zero_key);
            let one_direction = uut.direction(&one_key);

            assert_eq!(zero_direction, Direction::from(false));
            assert_eq!(one_direction, Direction::from(true));
        }

        #[test]
        fn get_child() {
            let left = Rc::new(RefCell::new(InternalNode::Unresolved(
                TrieStorageIndex::new(1).unwrap(),
            )));
            let right = Rc::new(RefCell::new(InternalNode::Unresolved(
                TrieStorageIndex::new(2).unwrap(),
            )));

            let uut = BinaryNode {
                storage_index: None,
                height: 1,
                left: left.clone(),
                right: right.clone(),
            };

            use Direction::*;
            assert_eq!(uut.get_child(Left), left);
            assert_eq!(uut.get_child(Right), right);
        }

        #[test]
        fn hash() {
            // Test data taken from starkware cairo-lang repo:
            // https://github.com/starkware-libs/cairo-lang/blob/fc97bdd8322a7df043c87c371634b26c15ed6cee/src/starkware/starkware_utils/commitment_tree/patricia_tree/nodes_test.py#L14
            //
            // Note that the hash function must be exchanged for `async_stark_hash_func`,
            // otherwise it just uses some other test hash function.
            let expected = Felt::from_hex_str(
                "0x0615bb8d47888d2987ad0c63fc06e9e771930986a4dd8adc55617febfcf3639e",
            )
            .unwrap();
            let left = felt!("0x1234");
            let right = felt!("0xabcd");

            let hash = BinaryNode::calculate_hash::<PedersenHash>(left, right);

            assert_eq!(hash, expected);
        }
    }

    mod edge {
        use bitvec::bitvec;
        use pathfinder_common::felt;

        use super::*;

        #[test]
        fn hash() {
            // Test data taken from starkware cairo-lang repo:
            // https://github.com/starkware-libs/cairo-lang/blob/fc97bdd8322a7df043c87c371634b26c15ed6cee/src/starkware/starkware_utils/commitment_tree/patricia_tree/nodes_test.py#L38
            //
            // Note that the hash function must be exchanged for `async_stark_hash_func`,
            // otherwise it just uses some other test hash function.
            let expected = Felt::from_hex_str(
                "0x1d937094c09b5f8e26a662d21911871e3cbc6858d55cc49af9848ea6fed4e9",
            )
            .unwrap();
            let child = felt!("0x1234ABCD");
            // Path = 42 in binary.
            let path = bitvec![u8, Msb0; 1, 0, 1, 0, 1, 0];

            let hash = EdgeNode::calculate_hash::<PedersenHash>(child, &path);

            assert_eq!(hash, expected);
        }

        mod path_matches {
            use pathfinder_common::felt;

            use super::*;

            #[test]
            fn full() {
                let key = felt!("0x123456789abcdef");
                let child = Rc::new(RefCell::new(InternalNode::Unresolved(
                    TrieStorageIndex::new(1).unwrap(),
                )));

                let uut = EdgeNode {
                    storage_index: None,
                    height: 0,
                    path: key.view_bits().to_bitvec(),
                    child,
                };

                assert!(uut.path_matches(key.view_bits()));
            }

            #[test]
            fn prefix() {
                let key = felt!("0x123456789abcdef");
                let child = Rc::new(RefCell::new(InternalNode::Unresolved(
                    TrieStorageIndex::new(1).unwrap(),
                )));

                let path = key.view_bits()[..45].to_bitvec();

                let uut = EdgeNode {
                    storage_index: None,
                    height: 0,
                    path,
                    child,
                };

                assert!(uut.path_matches(key.view_bits()));
            }

            #[test]
            fn suffix() {
                let key = felt!("0x123456789abcdef");
                let child = Rc::new(RefCell::new(InternalNode::Unresolved(
                    TrieStorageIndex::new(1).unwrap(),
                )));

                let path = key.view_bits()[50..].to_bitvec();

                let uut = EdgeNode {
                    storage_index: None,
                    height: 50,
                    path,
                    child,
                };

                assert!(uut.path_matches(key.view_bits()));
            }

            #[test]
            fn middle_slice() {
                let key = felt!("0x123456789abcdef");
                let child = Rc::new(RefCell::new(InternalNode::Unresolved(
                    TrieStorageIndex::new(1).unwrap(),
                )));

                let path = key.view_bits()[230..235].to_bitvec();

                let uut = EdgeNode {
                    storage_index: None,
                    height: 230,
                    path,
                    child,
                };

                assert!(uut.path_matches(key.view_bits()));
            }
        }
    }
}
