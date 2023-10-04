//! Provides high-level access to database shielded:
//! - nullifiers
//! - note commitment trees
//! - anchors
//!
//! This module makes sure that:
//! - all disk writes happen inside a RocksDB transaction, and
//! - format-specific invariants are maintained.
//!
//! # Correctness
//!
//! The [`crate::constants::DATABASE_FORMAT_VERSION`] constant must
//! be incremented each time the database format (column, serialization, etc) changes.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use zebra_chain::{
    block::Height,
    orchard,
    parallel::tree::NoteCommitmentTrees,
    sapling, sprout,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
    transaction::Transaction,
};

use crate::{
    request::SemanticallyVerifiedBlockWithTrees,
    service::finalized_state::{
        disk_db::{DiskDb, DiskWriteBatch, ReadDisk, WriteDisk},
        zebra_db::ZebraDb,
    },
    BoxError, SemanticallyVerifiedBlock,
};

// Doc-only items
#[allow(unused_imports)]
use zebra_chain::subtree::NoteCommitmentSubtree;

impl ZebraDb {
    // Read shielded methods

    /// Returns `true` if the finalized state contains `sprout_nullifier`.
    pub fn contains_sprout_nullifier(&self, sprout_nullifier: &sprout::Nullifier) -> bool {
        let sprout_nullifiers = self.db.cf_handle("sprout_nullifiers").unwrap();
        self.db.zs_contains(&sprout_nullifiers, &sprout_nullifier)
    }

    /// Returns `true` if the finalized state contains `sapling_nullifier`.
    pub fn contains_sapling_nullifier(&self, sapling_nullifier: &sapling::Nullifier) -> bool {
        let sapling_nullifiers = self.db.cf_handle("sapling_nullifiers").unwrap();
        self.db.zs_contains(&sapling_nullifiers, &sapling_nullifier)
    }

    /// Returns `true` if the finalized state contains `orchard_nullifier`.
    pub fn contains_orchard_nullifier(&self, orchard_nullifier: &orchard::Nullifier) -> bool {
        let orchard_nullifiers = self.db.cf_handle("orchard_nullifiers").unwrap();
        self.db.zs_contains(&orchard_nullifiers, &orchard_nullifier)
    }

    /// Returns `true` if the finalized state contains `sprout_anchor`.
    #[allow(unused)]
    pub fn contains_sprout_anchor(&self, sprout_anchor: &sprout::tree::Root) -> bool {
        let sprout_anchors = self.db.cf_handle("sprout_anchors").unwrap();
        self.db.zs_contains(&sprout_anchors, &sprout_anchor)
    }

    /// Returns `true` if the finalized state contains `sapling_anchor`.
    pub fn contains_sapling_anchor(&self, sapling_anchor: &sapling::tree::Root) -> bool {
        let sapling_anchors = self.db.cf_handle("sapling_anchors").unwrap();
        self.db.zs_contains(&sapling_anchors, &sapling_anchor)
    }

    /// Returns `true` if the finalized state contains `orchard_anchor`.
    pub fn contains_orchard_anchor(&self, orchard_anchor: &orchard::tree::Root) -> bool {
        let orchard_anchors = self.db.cf_handle("orchard_anchors").unwrap();
        self.db.zs_contains(&orchard_anchors, &orchard_anchor)
    }

    // # Sprout trees

    /// Returns the Sprout note commitment tree of the finalized tip
    /// or the empty tree if the state is empty.
    pub fn sprout_tree(&self) -> Arc<sprout::tree::NoteCommitmentTree> {
        if self.is_empty() {
            return Arc::<sprout::tree::NoteCommitmentTree>::default();
        }

        // # Performance
        //
        // Using `zs_last_key_value()` on this column family significantly reduces sync performance
        // (#7618). This is probably because `zs_delete()` is also used on the same column family.
        // See the comment in `ZebraDb::history_tree()` for details.
        //
        // This bug will be fixed by PR #7392, because it changes this column family to update the
        // existing key, rather than deleting old keys.
        let sprout_tree_cf = self.db.cf_handle("sprout_note_commitment_tree").unwrap();

        // # Forwards Compatibility
        //
        // This code can read the column family format in 1.2.0 and earlier (tip height key),
        // and after PR #7392 is merged (empty key). The height-based code can be removed when
        // versions 1.2.0 and earlier are no longer supported.
        //
        // # Concurrency
        //
        // There is only one tree in this column family, which is atomically updated by a block
        // write batch (database transaction). If this update runs between the height read and
        // the tree read, the height will be wrong, and the tree will be missing.
        // That could cause consensus bugs.
        //
        // See the comment in `ZebraDb::history_tree()` for details.
        //
        // TODO: this concurrency bug will be permanently fixed in PR #7392,
        //       by changing the block update to overwrite the tree, rather than deleting it.
        let mut sprout_tree: Option<Arc<sprout::tree::NoteCommitmentTree>> =
            self.db.zs_get(&sprout_tree_cf, &());

        if sprout_tree.is_none() {
            let tip_height = self
                .finalized_tip_height()
                .expect("just checked for an empty database");

            sprout_tree = self.db.zs_get(&sprout_tree_cf, &tip_height);
        }

        sprout_tree.expect("Sprout note commitment tree must exist if there is a finalized tip")
    }

    /// Returns the Sprout note commitment tree matching the given anchor.
    ///
    /// This is used for interstitial tree building, which is unique to Sprout.
    #[allow(clippy::unwrap_in_result)]
    pub fn sprout_tree_by_anchor(
        &self,
        sprout_anchor: &sprout::tree::Root,
    ) -> Option<Arc<sprout::tree::NoteCommitmentTree>> {
        let sprout_anchors_handle = self.db.cf_handle("sprout_anchors").unwrap();

        self.db
            .zs_get(&sprout_anchors_handle, sprout_anchor)
            .map(Arc::new)
    }

    /// Returns all the Sprout note commitment trees in the database.
    ///
    /// Calling this method can load a lot of data into RAM, and delay block commit transactions.
    #[allow(dead_code, clippy::unwrap_in_result)]
    pub fn sprout_trees_full_map(
        &self,
    ) -> HashMap<sprout::tree::Root, Arc<sprout::tree::NoteCommitmentTree>> {
        let sprout_anchors_handle = self.db.cf_handle("sprout_anchors").unwrap();

        self.db
            .zs_items_in_range_unordered(&sprout_anchors_handle, ..)
    }

    // # Sapling trees

    /// Returns the Sapling note commitment tree of the finalized tip or the empty tree if the state
    /// is empty.
    pub fn sapling_tree(&self) -> Arc<sapling::tree::NoteCommitmentTree> {
        let height = match self.finalized_tip_height() {
            Some(h) => h,
            None => return Default::default(),
        };

        self.sapling_tree_by_height(&height)
            .expect("Sapling note commitment tree must exist if there is a finalized tip")
    }

    /// Returns the Sapling note commitment tree matching the given block height, or `None` if the
    /// height is above the finalized tip.
    #[allow(clippy::unwrap_in_result)]
    pub fn sapling_tree_by_height(
        &self,
        height: &Height,
    ) -> Option<Arc<sapling::tree::NoteCommitmentTree>> {
        let tip_height = self.finalized_tip_height()?;

        // If we're above the tip, searching backwards would always return the tip tree.
        // But the correct answer is "we don't know that tree yet".
        if *height > tip_height {
            return None;
        }

        let sapling_trees = self.db.cf_handle("sapling_note_commitment_tree").unwrap();

        // If we know there must be a tree, search backwards for it.
        let (_first_duplicate_height, tree) = self
            .db
            .zs_prev_key_value_back_from(&sapling_trees, height)
            .expect(
                "Sapling note commitment trees must exist for all heights below the finalized tip",
            );

        Some(Arc::new(tree))
    }

    /// Returns the Sapling note commitment trees in the supplied range, in increasing height order.
    #[allow(clippy::unwrap_in_result)]
    pub fn sapling_tree_by_height_range<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (Height, Arc<sapling::tree::NoteCommitmentTree>)> + '_
    where
        R: std::ops::RangeBounds<Height>,
    {
        let sapling_trees = self.db.cf_handle("sapling_note_commitment_tree").unwrap();
        self.db.zs_range_iter(&sapling_trees, range)
    }

    /// Returns the Sapling note commitment trees in the reversed range, in decreasing height order.
    #[allow(clippy::unwrap_in_result)]
    pub fn sapling_tree_by_reversed_height_range<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (Height, Arc<sapling::tree::NoteCommitmentTree>)> + '_
    where
        R: std::ops::RangeBounds<Height>,
    {
        let sapling_trees = self.db.cf_handle("sapling_note_commitment_tree").unwrap();
        self.db.zs_reverse_range_iter(&sapling_trees, range)
    }

    /// Returns the Sapling note commitment subtree at this `index`.
    ///
    /// # Correctness
    ///
    /// This method should not be used to get subtrees for RPC responses,
    /// because those subtree lists require that the start subtree is present in the list.
    /// Instead, use `sapling_subtree_list_by_index_for_rpc()`.
    #[allow(clippy::unwrap_in_result)]
    pub(in super::super) fn sapling_subtree_by_index(
        &self,
        index: impl Into<NoteCommitmentSubtreeIndex> + Copy,
    ) -> Option<NoteCommitmentSubtree<sapling::tree::Node>> {
        let sapling_subtrees = self
            .db
            .cf_handle("sapling_note_commitment_subtree")
            .unwrap();

        let subtree_data: NoteCommitmentSubtreeData<sapling::tree::Node> =
            self.db.zs_get(&sapling_subtrees, &index.into())?;

        Some(subtree_data.with_index(index))
    }

    /// Returns a list of Sapling [`NoteCommitmentSubtree`]s starting at `start_index`.
    /// If `limit` is provided, the list is limited to `limit` entries.
    ///
    /// If there is no subtree at `start_index`, the returned list is empty.
    /// Otherwise, subtrees are continuous up to the finalized tip.
    ///
    /// # Correctness
    ///
    /// This method is specifically designed for the `z_getsubtreesbyindex` state request.
    /// It might not work for other RPCs or state checks.
    #[allow(clippy::unwrap_in_result)]
    pub fn sapling_subtree_list_by_index_for_rpc(
        &self,
        start_index: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<sapling::tree::Node>> {
        let sapling_subtrees = self
            .db
            .cf_handle("sapling_note_commitment_subtree")
            .unwrap();

        // Calculate the end bound, checking for overflow.
        let exclusive_end_bound: Option<NoteCommitmentSubtreeIndex> = limit
            .and_then(|limit| start_index.0.checked_add(limit.0))
            .map(NoteCommitmentSubtreeIndex);

        let list: BTreeMap<
            NoteCommitmentSubtreeIndex,
            NoteCommitmentSubtreeData<sapling::tree::Node>,
        >;

        if let Some(exclusive_end_bound) = exclusive_end_bound {
            list = self
                .db
                .zs_range_iter(&sapling_subtrees, start_index..exclusive_end_bound)
                .collect();
        } else {
            // If there is no end bound, just return all the trees.
            // If the end bound would overflow, just returns all the trees, because that's what
            // `zcashd` does. (It never calculates an end bound, so it just keeps iterating until
            // the trees run out.)
            list = self
                .db
                .zs_range_iter(&sapling_subtrees, start_index..)
                .collect();
        }

        // Check that we got the start subtree.
        if list.get(&start_index).is_some() {
            list
        } else {
            BTreeMap::new()
        }
    }

    // Orchard trees

    /// Returns the Orchard note commitment tree of the finalized tip or the empty tree if the state
    /// is empty.
    pub fn orchard_tree(&self) -> Arc<orchard::tree::NoteCommitmentTree> {
        let height = match self.finalized_tip_height() {
            Some(h) => h,
            None => return Default::default(),
        };

        self.orchard_tree_by_height(&height)
            .expect("Orchard note commitment tree must exist if there is a finalized tip")
    }

    /// Returns the Orchard note commitment tree matching the given block height,
    /// or `None` if the height is above the finalized tip.
    #[allow(clippy::unwrap_in_result)]
    pub fn orchard_tree_by_height(
        &self,
        height: &Height,
    ) -> Option<Arc<orchard::tree::NoteCommitmentTree>> {
        let tip_height = self.finalized_tip_height()?;

        // If we're above the tip, searching backwards would always return the tip tree.
        // But the correct answer is "we don't know that tree yet".
        if *height > tip_height {
            return None;
        }

        let orchard_trees = self.db.cf_handle("orchard_note_commitment_tree").unwrap();

        // If we know there must be a tree, search backwards for it.
        let (_first_duplicate_height, tree) = self
            .db
            .zs_prev_key_value_back_from(&orchard_trees, height)
            .expect(
                "Orchard note commitment trees must exist for all heights below the finalized tip",
            );

        Some(Arc::new(tree))
    }

    /// Returns the Orchard note commitment trees in the supplied range, in increasing height order.
    #[allow(clippy::unwrap_in_result)]
    pub fn orchard_tree_by_height_range<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (Height, Arc<orchard::tree::NoteCommitmentTree>)> + '_
    where
        R: std::ops::RangeBounds<Height>,
    {
        let orchard_trees = self.db.cf_handle("orchard_note_commitment_tree").unwrap();
        self.db.zs_range_iter(&orchard_trees, range)
    }

    /// Returns the Orchard note commitment trees in the reversed range, in decreasing height order.
    #[allow(clippy::unwrap_in_result)]
    pub fn orchard_tree_by_reversed_height_range<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = (Height, Arc<orchard::tree::NoteCommitmentTree>)> + '_
    where
        R: std::ops::RangeBounds<Height>,
    {
        let orchard_trees = self.db.cf_handle("orchard_note_commitment_tree").unwrap();
        self.db.zs_reverse_range_iter(&orchard_trees, range)
    }

    /// Returns the Orchard note commitment subtree at this `index`.
    ///
    /// # Correctness
    ///
    /// This method should not be used to get subtrees for RPC responses,
    /// because those subtree lists require that the start subtree is present in the list.
    /// Instead, use `orchard_subtree_list_by_index_for_rpc()`.
    #[allow(clippy::unwrap_in_result)]
    pub(in super::super) fn orchard_subtree_by_index(
        &self,
        index: impl Into<NoteCommitmentSubtreeIndex> + Copy,
    ) -> Option<NoteCommitmentSubtree<orchard::tree::Node>> {
        let orchard_subtrees = self
            .db
            .cf_handle("orchard_note_commitment_subtree")
            .unwrap();

        let subtree_data: NoteCommitmentSubtreeData<orchard::tree::Node> =
            self.db.zs_get(&orchard_subtrees, &index.into())?;

        Some(subtree_data.with_index(index))
    }

    /// Returns a list of Orchard [`NoteCommitmentSubtree`]s starting at `start_index`.
    /// If `limit` is provided, the list is limited to `limit` entries.
    ///
    /// If there is no subtree at `start_index`, the returned list is empty.
    /// Otherwise, subtrees are continuous up to the finalized tip.
    ///
    /// # Correctness
    ///
    /// This method is specifically designed for the `z_getsubtreesbyindex` state request.
    /// It might not work for other RPCs or state checks.
    #[allow(clippy::unwrap_in_result)]
    pub fn orchard_subtree_list_by_index_for_rpc(
        &self,
        start_index: NoteCommitmentSubtreeIndex,
        limit: Option<NoteCommitmentSubtreeIndex>,
    ) -> BTreeMap<NoteCommitmentSubtreeIndex, NoteCommitmentSubtreeData<orchard::tree::Node>> {
        let orchard_subtrees = self
            .db
            .cf_handle("orchard_note_commitment_subtree")
            .unwrap();

        // Calculate the end bound, checking for overflow.
        let exclusive_end_bound: Option<NoteCommitmentSubtreeIndex> = limit
            .and_then(|limit| start_index.0.checked_add(limit.0))
            .map(NoteCommitmentSubtreeIndex);

        let list: BTreeMap<
            NoteCommitmentSubtreeIndex,
            NoteCommitmentSubtreeData<orchard::tree::Node>,
        >;

        if let Some(exclusive_end_bound) = exclusive_end_bound {
            list = self
                .db
                .zs_range_iter(&orchard_subtrees, start_index..exclusive_end_bound)
                .collect();
        } else {
            // If there is no end bound, just return all the trees.
            // If the end bound would overflow, just returns all the trees, because that's what
            // `zcashd` does. (It never calculates an end bound, so it just keeps iterating until
            // the trees run out.)
            list = self
                .db
                .zs_range_iter(&orchard_subtrees, start_index..)
                .collect();
        }

        // Check that we got the start subtree.
        if list.get(&start_index).is_some() {
            list
        } else {
            BTreeMap::new()
        }
    }

    /// Returns the shielded note commitment trees of the finalized tip
    /// or the empty trees if the state is empty.
    pub fn note_commitment_trees(&self) -> NoteCommitmentTrees {
        NoteCommitmentTrees {
            sprout: self.sprout_tree(),
            sapling: self.sapling_tree(),
            sapling_subtree: None,
            orchard: self.orchard_tree(),
            orchard_subtree: None,
        }
    }
}

impl DiskWriteBatch {
    /// Prepare a database batch containing `finalized.block`'s shielded transaction indexes,
    /// and return it (without actually writing anything).
    ///
    /// If this method returns an error, it will be propagated,
    /// and the batch should not be written to the database.
    ///
    /// # Errors
    ///
    /// - Propagates any errors from updating note commitment trees
    pub fn prepare_shielded_transaction_batch(
        &mut self,
        db: &DiskDb,
        finalized: &SemanticallyVerifiedBlock,
    ) -> Result<(), BoxError> {
        let SemanticallyVerifiedBlock { block, .. } = finalized;

        // Index each transaction's shielded data
        for transaction in &block.transactions {
            self.prepare_nullifier_batch(db, transaction)?;
        }

        Ok(())
    }

    /// Prepare a database batch containing `finalized.block`'s nullifiers,
    /// and return it (without actually writing anything).
    ///
    /// # Errors
    ///
    /// - This method doesn't currently return any errors, but it might in future
    #[allow(clippy::unwrap_in_result)]
    pub fn prepare_nullifier_batch(
        &mut self,
        db: &DiskDb,
        transaction: &Transaction,
    ) -> Result<(), BoxError> {
        let sprout_nullifiers = db.cf_handle("sprout_nullifiers").unwrap();
        let sapling_nullifiers = db.cf_handle("sapling_nullifiers").unwrap();
        let orchard_nullifiers = db.cf_handle("orchard_nullifiers").unwrap();

        // Mark sprout, sapling and orchard nullifiers as spent
        for sprout_nullifier in transaction.sprout_nullifiers() {
            self.zs_insert(&sprout_nullifiers, sprout_nullifier, ());
        }
        for sapling_nullifier in transaction.sapling_nullifiers() {
            self.zs_insert(&sapling_nullifiers, sapling_nullifier, ());
        }
        for orchard_nullifier in transaction.orchard_nullifiers() {
            self.zs_insert(&orchard_nullifiers, orchard_nullifier, ());
        }

        Ok(())
    }

    /// Prepare a database batch containing the note commitment and history tree updates
    /// from `finalized.block`, and return it (without actually writing anything).
    ///
    /// If this method returns an error, it will be propagated,
    /// and the batch should not be written to the database.
    ///
    /// # Errors
    ///
    /// - Propagates any errors from updating the history tree
    #[allow(clippy::unwrap_in_result)]
    pub fn prepare_trees_batch(
        &mut self,
        zebra_db: &ZebraDb,
        finalized: &SemanticallyVerifiedBlockWithTrees,
        prev_note_commitment_trees: Option<NoteCommitmentTrees>,
    ) -> Result<(), BoxError> {
        let db = &zebra_db.db;

        let sprout_anchors = db.cf_handle("sprout_anchors").unwrap();
        let sapling_anchors = db.cf_handle("sapling_anchors").unwrap();
        let orchard_anchors = db.cf_handle("orchard_anchors").unwrap();

        let sprout_tree_cf = db.cf_handle("sprout_note_commitment_tree").unwrap();
        let sapling_tree_cf = db.cf_handle("sapling_note_commitment_tree").unwrap();
        let orchard_tree_cf = db.cf_handle("orchard_note_commitment_tree").unwrap();

        let height = finalized.verified.height;
        let trees = finalized.treestate.note_commitment_trees.clone();

        // Use the cached values that were previously calculated in parallel.
        let sprout_root = trees.sprout.root();
        let sapling_root = trees.sapling.root();
        let orchard_root = trees.orchard.root();

        // Index the new anchors.
        // Note: if the root hasn't changed, we write the same value again.
        self.zs_insert(&sprout_anchors, sprout_root, &trees.sprout);
        self.zs_insert(&sapling_anchors, sapling_root, ());
        self.zs_insert(&orchard_anchors, orchard_root, ());

        // Delete the previously stored Sprout note commitment tree.
        let current_tip_height = height - 1;
        if let Some(h) = current_tip_height {
            self.zs_delete(&sprout_tree_cf, h);
        }

        // TODO: if we ever need concurrent read-only access to the sprout tree,
        // store it by `()`, not height. Otherwise, the ReadStateService could
        // access a height that was just deleted by a concurrent StateService
        // write. This requires a database version update.
        self.zs_insert(&sprout_tree_cf, height, trees.sprout);

        // Store the Sapling tree only if it is not already present at the previous height.
        if height.is_min()
            || prev_note_commitment_trees
                .as_ref()
                .map_or_else(|| zebra_db.sapling_tree(), |trees| trees.sapling.clone())
                != trees.sapling
        {
            self.zs_insert(&sapling_tree_cf, height, trees.sapling);
        }

        // Store the Orchard tree only if it is not already present at the previous height.
        if height.is_min()
            || prev_note_commitment_trees
                .map_or_else(|| zebra_db.orchard_tree(), |trees| trees.orchard)
                != trees.orchard
        {
            self.zs_insert(&orchard_tree_cf, height, trees.orchard);
        }

        if let Some(subtree) = trees.sapling_subtree {
            self.insert_sapling_subtree(zebra_db, &subtree);
        }

        if let Some(subtree) = trees.orchard_subtree {
            self.insert_orchard_subtree(zebra_db, &subtree);
        }

        self.prepare_history_batch(db, finalized)
    }

    // Sapling tree methods

    /// Inserts the Sapling note commitment subtree.
    pub fn insert_sapling_subtree(
        &mut self,
        zebra_db: &ZebraDb,
        subtree: &NoteCommitmentSubtree<sapling::tree::Node>,
    ) {
        let sapling_subtree_cf = zebra_db
            .db
            .cf_handle("sapling_note_commitment_subtree")
            .unwrap();
        self.zs_insert(&sapling_subtree_cf, subtree.index, subtree.into_data());
    }

    /// Deletes the Sapling note commitment tree at the given [`Height`].
    pub fn delete_sapling_tree(&mut self, zebra_db: &ZebraDb, height: &Height) {
        let sapling_tree_cf = zebra_db
            .db
            .cf_handle("sapling_note_commitment_tree")
            .unwrap();
        self.zs_delete(&sapling_tree_cf, height);
    }

    /// Deletes the range of Sapling note commitment trees at the given [`Height`]s. Doesn't delete the upper bound.
    #[allow(dead_code)]
    pub fn delete_range_sapling_tree(&mut self, zebra_db: &ZebraDb, from: &Height, to: &Height) {
        let sapling_tree_cf = zebra_db
            .db
            .cf_handle("sapling_note_commitment_tree")
            .unwrap();

        // TODO: convert zs_delete_range() to take std::ops::RangeBounds
        self.zs_delete_range(&sapling_tree_cf, from, to);
    }

    /// Deletes the range of Sapling subtrees at the given [`NoteCommitmentSubtreeIndex`]es.
    /// Doesn't delete the upper bound.
    pub fn delete_range_sapling_subtree(
        &mut self,
        zebra_db: &ZebraDb,
        from: NoteCommitmentSubtreeIndex,
        to: NoteCommitmentSubtreeIndex,
    ) {
        let sapling_subtree_cf = zebra_db
            .db
            .cf_handle("sapling_note_commitment_subtree")
            .unwrap();

        // TODO: convert zs_delete_range() to take std::ops::RangeBounds
        self.zs_delete_range(&sapling_subtree_cf, from, to);
    }

    // Orchard tree methods

    /// Inserts the Orchard note commitment subtree.
    pub fn insert_orchard_subtree(
        &mut self,
        zebra_db: &ZebraDb,
        subtree: &NoteCommitmentSubtree<orchard::tree::Node>,
    ) {
        let orchard_subtree_cf = zebra_db
            .db
            .cf_handle("orchard_note_commitment_subtree")
            .unwrap();
        self.zs_insert(&orchard_subtree_cf, subtree.index, subtree.into_data());
    }

    /// Deletes the Orchard note commitment tree at the given [`Height`].
    pub fn delete_orchard_tree(&mut self, zebra_db: &ZebraDb, height: &Height) {
        let orchard_tree_cf = zebra_db
            .db
            .cf_handle("orchard_note_commitment_tree")
            .unwrap();
        self.zs_delete(&orchard_tree_cf, height);
    }

    /// Deletes the range of Orchard note commitment trees at the given [`Height`]s. Doesn't delete the upper bound.
    #[allow(dead_code)]
    pub fn delete_range_orchard_tree(&mut self, zebra_db: &ZebraDb, from: &Height, to: &Height) {
        let orchard_tree_cf = zebra_db
            .db
            .cf_handle("orchard_note_commitment_tree")
            .unwrap();

        // TODO: convert zs_delete_range() to take std::ops::RangeBounds
        self.zs_delete_range(&orchard_tree_cf, from, to);
    }

    /// Deletes the range of Orchard subtrees at the given [`NoteCommitmentSubtreeIndex`]es.
    /// Doesn't delete the upper bound.
    pub fn delete_range_orchard_subtree(
        &mut self,
        zebra_db: &ZebraDb,
        from: NoteCommitmentSubtreeIndex,
        to: NoteCommitmentSubtreeIndex,
    ) {
        let orchard_subtree_cf = zebra_db
            .db
            .cf_handle("orchard_note_commitment_subtree")
            .unwrap();

        // TODO: convert zs_delete_range() to take std::ops::RangeBounds
        self.zs_delete_range(&orchard_subtree_cf, from, to);
    }
}
