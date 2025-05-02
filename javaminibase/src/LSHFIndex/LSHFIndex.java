package LSHFIndex;

import global.*;
import heap.*; // Assuming RID is here
import java.io.*;
import java.util.*;

/**
 * Represents the overall LSH index structure, composed of multiple layers,
 * each potentially represented by a PrefixTree or similar structure.
 */
public class LSHFIndex implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L; // Recommended for Serializable classes

    private int h; // Number of hash functions per layer (or bits for prefix tree)
    private int L; // Number of layers (hash tables/trees)
    private List<PrefixTree> layers; // List of prefix trees (one per layer)
    // private String relationName; // Optional: Store relation name if needed for Heapfile access within index methods

    // Constructor
    public LSHFIndex(int h, int L /*, String relationName */) {
        this.h = h;
        this.L = L;
        // this.relationName = relationName; // Store if needed
        this.layers = new ArrayList<>();
        int baseSeed = 12345; // Fixed base seed for reproducibility across runs
        for (int i = 0; i < L; i++) {
            // Each layer gets a unique seed derived from the base seed
            layers.add(new PrefixTree(baseSeed + i, h));
        }
    }

    /**
     * Inserts a key-RID pair into all layers of the index.
     * @param key The vector key to insert.
     * @param rid The Record ID associated with the key.
     * @throws IOException If an I/O error occurs during insertion in any layer.
     */
    public void insert(Vector100DKey key, RID rid) throws IOException {
        Vector100Dtype vector = key.getVector();
        for (PrefixTree layer : layers) {
            // Compute hash specific to this layer (using its seed)
            int hashValue = layer.computeHash(vector);
            // Insert into the specific layer's structure
            layer.insert(hashValue, vector, rid);
        }
    }

    /**
     * Deletes a key-RID pair from all layers of the index.
     * Note: Assumes the key exists and matches the RID. Error handling might be needed.
     * @param key The vector key to delete.
     * @param rid The Record ID associated with the key to delete.
     * @throws IOException If an I/O error occurs during deletion in any layer.
     */
    public void delete(Vector100DKey key, RID rid) throws IOException {
        Vector100Dtype vector = key.getVector();
        for (PrefixTree layer : layers) {
            // Compute hash specific to this layer
            int hashValue = layer.computeHash(vector);
            // Delete from the specific layer's structure
            layer.delete(hashValue, vector, rid);
        }
    }

    /**
     * Performs a range search across all layers, returning unique candidate RIDs.
     * LSH provides candidates; further verification (actual distance check) is needed.
     * @param target The query vector key.
     * @param distance The maximum distance for the range search (used by layer search).
     * @return A List of unique candidate RIDs found across all layers.
     * @throws IOException If an I/O error occurs during search in any layer.
     */
    public List<RID> rangeSearch(Vector100DKey target, int distance) throws IOException {
        Vector100Dtype vector = target.getVector();
        // Use a HashSet to automatically handle duplicates collected from different layers
        Set<RID> uniqueResults = new HashSet<>();
        for (PrefixTree layer : layers) {
            // Add all candidate RIDs found in this layer to the set.
            // Duplicates (RIDs already present from other layers) will be ignored by the HashSet.
            uniqueResults.addAll(layer.rangeSearch(vector, distance));
        }
        // Convert the set of unique RIDs back to a List for the return type.
        // Note: This list contains *candidates*. The actual distance check
        // should happen externally (e.g., in RSIndexScan) by fetching tuples.
        return new ArrayList<>(uniqueResults);
    }

    /**
     * Performs a nearest-neighbor search across all layers.
     * Note: This implementation aggregates results from layers but might suffer from
     * LSH approximation issues (not guaranteeing true K-NN) and potential duplicates
     * if layer.nearestNeighborSearch returns pairs with the same RID.
     * A more robust implementation would collect unique candidate RIDs, fetch tuples,
     * calculate exact distances, and then find the top K.
     * @param target The query vector key.
     * @param k The number of nearest neighbors to find (0 means find all candidates).
     * @return A List of RIDDistancePair objects, potentially approximate neighbors.
     * @throws IOException If an I/O error occurs during search in any layer.
     */
    public List<RIDDistancePair> nearestNeighborSearch(Vector100DKey target, int k) throws IOException {
        Vector100Dtype vector = target.getVector();
        // Use a PriorityQueue to store potential neighbors, ordered by distance.
        // Note: Comparator.comparingDouble sorts ascending (smallest distance first).
        PriorityQueue<RIDDistancePair> pq = new PriorityQueue<>(Comparator.comparingDouble(p -> p.distance));

        // Aggregate candidate pairs from all layers
        for (PrefixTree layer : layers) {
            // Assuming layer.nearestNeighborSearch returns List<RIDDistancePair>
            // containing candidates found *within that layer*.
            // pq.addAll might add pairs with the same RID if found in multiple layers.
            pq.addAll(layer.nearestNeighborSearch(vector, k));
        }

        // Extract results from the priority queue
        List<RIDDistancePair> results = new ArrayList<>();
        Set<RID> seenRids = new HashSet<>(); // Keep track to return unique RIDs

        // Extract up to k unique RIDs or all if k=0
        while (!pq.isEmpty()) {
            RIDDistancePair current = pq.poll();
            // Add if k=0 OR we haven't reached k results yet, AND we haven't seen this RID before
            if ((k == 0 || results.size() < k) && seenRids.add(current.rid)) {
                 results.add(current);
            }
            // If k > 0 and we already have k results, we can stop early
            // because the PQ gives us the smallest distances first.
            if (k > 0 && results.size() >= k) {
                break;
            }
        }
        // The results list is already sorted by distance due to polling from the PQ.
        return results;
    }


    /**
     * Saves the current state of the LSH index (including all layers) to a file.
     * @param filePath The path to the file where the index should be saved.
     * @throws IOException If an I/O error occurs during serialization.
     */
    public void saveIndex(String filePath) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeObject(this); // Serialize the entire LSHFIndex object
        }
    }

    /**
     * Loads an LSH index from a previously saved file.
     * @param filePath The path to the file containing the serialized index.
     * @return The deserialized LSHFIndex object.
     * @throws IOException If an I/O error occurs during deserialization.
     * @throws ClassNotFoundException If the class definition for the serialized object cannot be found.
     */
    public static LSHFIndex loadIndex(String filePath) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            return (LSHFIndex) ois.readObject(); // Deserialize the object
        }
    }

    // Getters might be useful for debugging or information
    public int getH() {
        return h;
    }

    public int getL() {
        return L;
    }
}