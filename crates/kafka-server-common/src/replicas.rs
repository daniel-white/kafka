//! Replicas utility: helpers for manipulating replica ID arrays.
//!
//! Mirrors Java's `Replicas` utility class — all static methods.
//!
//! MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java

/// Empty replica slice.
///
/// Mirrors Java's `Replicas.NONE`.
///
/// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
pub const NONE: &[i32] = &[];

/// Replica array utilities.
///
/// All methods are static — mirrors Java's `Replicas` utility class.
///
/// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
pub struct Replicas;

impl Replicas {
    /// An empty replica array.
    ///
    /// Mirrors Java's `Replicas.NONE`.
    pub const NONE_ARRAY: &'static [i32] = &[];

    /// Convert a slice of integers to a Vec.
    ///
    /// Mirrors Java's `Replicas.toList(int[])`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
    pub fn to_list(array: &[i32]) -> Vec<i32> {
        array.to_vec()
    }

    /// Convert a slice of integers to a Vec.
    ///
    /// Mirrors Java's `Replicas.toArray(List<Integer>)`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
    pub fn to_array(list: &[i32]) -> Vec<i32> {
        list.to_vec()
    }

    /// Copy a replica slice.
    ///
    /// Mirrors Java's `Replicas.clone(int[])`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
    pub fn clone(array: &[i32]) -> Vec<i32> {
        array.to_vec()
    }

    /// Check that a replica set is valid: no negatives, no duplicates.
    ///
    /// Mirrors Java's `Replicas.validate(int[])`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
    pub fn validate(replicas: &[i32]) -> bool {
        if replicas.is_empty() {
            return true;
        }
        let mut sorted: Vec<i32> = replicas.to_vec();
        sorted.sort_unstable();
        if sorted[0] < 0 {
            return false;
        }
        for i in 1..sorted.len() {
            if sorted[i - 1] == sorted[i] {
                return false;
            }
        }
        true
    }

    /// Check that an ISR set is valid.
    ///
    /// Mirrors Java's `Replicas.validateIsr(int[], int[])`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
    pub fn validate_isr(replicas: &[i32], isr: &[i32]) -> bool {
        if isr.is_empty() {
            return true;
        }
        if replicas.is_empty() {
            return false;
        }
        let mut sorted_replicas: Vec<i32> = replicas.to_vec();
        sorted_replicas.sort_unstable();
        let mut sorted_isr: Vec<i32> = isr.to_vec();
        sorted_isr.sort_unstable();

        let mut j = 0;
        if sorted_isr[0] < 0 {
            return false;
        }
        let mut prev_isr = -1;
        for &cur_isr in &sorted_isr {
            if prev_isr == cur_isr {
                return false;
            }
            prev_isr = cur_isr;
            loop {
                if j == sorted_replicas.len() {
                    return false;
                }
                let cur_replica = sorted_replicas[j];
                j += 1;
                if cur_replica == cur_isr {
                    break;
                }
            }
        }
        true
    }

    /// Returns true if a replica slice contains a specific value.
    ///
    /// Mirrors Java's `Replicas.contains(int[], int)`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
    pub fn contains(replicas: &[i32], value: i32) -> bool {
        replicas.contains(&value)
    }

    /// Check if slice `a` contains all values in slice `b`.
    ///
    /// Mirrors Java's `Replicas.contains(List<Integer>, int[])`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
    pub fn contains_slice(a: &[i32], b: &[i32]) -> bool {
        let mut a_sorted: Vec<i32> = a.to_vec();
        a_sorted.sort_unstable();
        let mut b_sorted: Vec<i32> = b.to_vec();
        b_sorted.sort_unstable();
        let mut i = 0;
        for &replica in &b_sorted {
            loop {
                if i >= a_sorted.len() {
                    return false;
                }
                let replica2 = a_sorted[i];
                i += 1;
                if replica2 == replica {
                    break;
                }
                if replica2 > replica {
                    return false;
                }
            }
        }
        true
    }

    /// Copy a replica slice without any occurrences of the given value.
    ///
    /// Mirrors Java's `Replicas.copyWithout(int[], int)`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
    pub fn copy_without(replicas: &[i32], value: i32) -> Vec<i32> {
        replicas.iter().copied().filter(|&r| r != value).collect()
    }

    /// Copy a replica slice without any occurrences of the given values.
    ///
    /// Mirrors Java's `Replicas.copyWithout(int[], int[])`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
    pub fn copy_without_slice(replicas: &[i32], values: &[i32]) -> Vec<i32> {
        replicas
            .iter()
            .copied()
            .filter(|&r| !Self::contains(values, r))
            .collect()
    }

    /// Copy a replica slice with the given value appended.
    ///
    /// Mirrors Java's `Replicas.copyWith(int[], int)`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
    pub fn copy_with(replicas: &[i32], value: i32) -> Vec<i32> {
        let mut result = replicas.to_vec();
        result.push(value);
        result
    }

    /// Convert a replica slice to a HashSet.
    ///
    /// Mirrors Java's `Replicas.toSet(int[])`.
    ///
    /// MIGRATION_SOURCE: metadata/src/main/java/org/apache/kafka/metadata/Replicas.java
    pub fn to_set(replicas: &[i32]) -> std::collections::HashSet<i32> {
        replicas.iter().copied().collect()
    }
}
