package LSHFIndex;

import global.*;
import heap.*;
import java.io.Serializable;
import java.util.*;

public class PrefixTree implements Serializable {
    private static final long serialVersionUID = 1L;
    
    // Key structures
    private Map<Integer, Map<Vector100Dtype, List<RID>>> tree; // Maps hash values to vectors to RIDs
    private int[][] hyperplanes; // Random hyperplanes for hash computation
    private int h; // Number of hash functions to use
    
    // Constructor with seed and h parameter
    public PrefixTree(int seed, int h) {
        this.h = h;
        this.tree = new HashMap<>();
        this.hyperplanes = new int[h][100]; // h hash functions for 100 dimensions
        
        // Initialize hyperplanes with random values
        Random random = new Random(seed);
        for (int i = 0; i < h; i++) {
            for (int j = 0; j < 100; j++) {
                hyperplanes[i][j] = random.nextInt(20001) - 10000; // Range [-10000, 10000]
            }
        }
    }
    
    // Constructor with just seed (default h=8)
    public PrefixTree(int seed) {
        this(seed, 8); // Default to 8 hash functions if not specified
    }

    // Compute the hash value for a key
    public int computeHash(Vector100Dtype key) {
        int[] vector = key.getValues();
        int hashValue = 0;
        
        // Use all h hyperplanes to generate an h-bit hash value
        for (int i = 0; i < h; i++) {
            long dotProduct = 0;
            for (int j = 0; j < vector.length; j++) {
                dotProduct += (long) vector[j] * hyperplanes[i][j];
            }
            
            // Set the i-th bit of the hash value based on hyperplane i
            if (dotProduct >= 0) {
                hashValue |= (1 << i); // Set bit if positive side of hyperplane
            }
        }
        
        return hashValue;
    }

    // Insert a key-RID pair into the prefix tree
    public void insert(int hashValue, Vector100Dtype key, RID rid) {
        // Get or create the map for this hash value
        Map<Vector100Dtype, List<RID>> vectorMap = tree.computeIfAbsent(hashValue, k -> new HashMap<>());
        
        // Get or create the list of RIDs for this vector
        List<RID> ridList = vectorMap.computeIfAbsent(key, k -> new ArrayList<>());
        
        // Add the RID to the list
        ridList.add(rid);
    }

    // Delete a key-RID pair from the prefix tree
    public void delete(int hashValue, Vector100Dtype key, RID rid) {
        Map<Vector100Dtype, List<RID>> vectorMap = tree.get(hashValue);
        if (vectorMap != null) {
            List<RID> ridList = vectorMap.get(key);
            if (ridList != null) {
                ridList.remove(rid);
                if (ridList.isEmpty()) {
                    vectorMap.remove(key);
                    if (vectorMap.isEmpty()) {
                        tree.remove(hashValue);
                    }
                }
            }
        }
    }

    // Perform a range search - compare actual vector distances, not hash differences
    public List<RID> rangeSearch(Vector100Dtype target, int distance) {
        List<RID> results = new ArrayList<>();
        int targetHash = computeHash(target);
        
        // First, collect candidates with potential matches (using hamming distance)
        Set<Integer> candidateHashes = new HashSet<>();
        for (Integer hashValue : tree.keySet()) {
            // Use hamming distance between hash codes as initial filter 
            // (much faster than checking every vector)
            if (hammingDistance(hashValue, targetHash) <= h/2) {
                candidateHashes.add(hashValue);
            }
        }
        
        // Process candidate hashes
        for (Integer hashValue : candidateHashes) {
            Map<Vector100Dtype, List<RID>> vectorMap = tree.get(hashValue);
            if (vectorMap != null) {
                for (Map.Entry<Vector100Dtype, List<RID>> entry : vectorMap.entrySet()) {
                    Vector100Dtype vector = entry.getKey();
                    // Use actual vector distance for accurate filtering
                    double actualDistance = vector.distanceTo(target);
                    if (actualDistance <= distance) {
                        results.addAll(entry.getValue());
                    }
                }
            }
        }
        
        return results;
    }

    // Perform a nearest-neighbor search using actual vector distances
    public List<RIDDistancePair> nearestNeighborSearch(Vector100Dtype target, int k) {
        // Use a Max Heap based on distance. Stores the k *smallest* distances found so far.
        // The largest of these k smallest distances will be at the top (peek).
        PriorityQueue<RIDDistancePair> pq = new PriorityQueue<>(
            Comparator.comparingDouble((RIDDistancePair p) -> p.distance).reversed() // Max heap based on distance
        );

        int targetHash = computeHash(target);

        // Iterate through potential candidate buckets based on hash proximity (optional optimization)
        // For simplicity and correctness, we can iterate through all entries if needed,
        // but filtering by Hamming distance is a common LSH strategy.
        for (Map.Entry<Integer, Map<Vector100Dtype, List<RID>>> entry : tree.entrySet()) {
            int hashValue = entry.getKey();

            // Optional: Filter buckets by Hamming distance (adjust threshold as needed)
            // if (hammingDistance(hashValue, targetHash) > h / 2) { // Example threshold
            //     continue; // Skip buckets likely too far
            // }

            // Process vectors within the candidate bucket
            for (Map.Entry<Vector100Dtype, List<RID>> vectorEntry : entry.getValue().entrySet()) {
                Vector100Dtype vector = vectorEntry.getKey();
                // Calculate the actual Euclidean distance
                double actualDistance = vector.distanceTo(target);

                for (RID rid : vectorEntry.getValue()) {
                    if (k == 0) {
                        // If k=0, add all candidates found in this layer
                        pq.add(new RIDDistancePair(rid, actualDistance));
                    } else {
                        // If k > 0, maintain only the k smallest distances
                        if (pq.size() < k) {
                            // If the heap isn't full, just add the new pair
                            pq.add(new RIDDistancePair(rid, actualDistance));
                        } else {
                            // If the heap is full, check if the new distance is smaller
                            // than the largest distance currently in the heap (pq.peek()).
                            if (actualDistance < pq.peek().distance) {
                                // If smaller, remove the largest element and add the new one.
                                pq.poll(); // Remove the largest
                                pq.add(new RIDDistancePair(rid, actualDistance)); // Add the new smaller one
                            }
                            // Otherwise, the new distance is larger than all k elements currently
                            // in the heap, so we discard it.
                        }
                    }
                }
            }
        }

        // Convert the priority queue (max heap) to a sorted list (ascending distance)
        List<RIDDistancePair> results = new ArrayList<>(pq.size());
        while (!pq.isEmpty()) {
            results.add(pq.poll()); // Poll elements (largest distance first)
        }
        // Reverse the list to get ascending order (smallest distance first)
        Collections.reverse(results);

        return results;
    }
    
    // Helper method: compute Hamming distance between two integers
    private int hammingDistance(int x, int y) {
        int xor = x ^ y;
        int distance = 0;
        while (xor != 0) {
            distance += xor & 1;
            xor >>= 1;
        }
        return distance;
    }
    
    // Get number of entries in the tree (for debugging/stats)
    public int size() {
        int count = 0;
        for (Map<Vector100Dtype, List<RID>> vectorMap : tree.values()) {
            for (List<RID> ridList : vectorMap.values()) {
                count += ridList.size();
            }
        }
        return count;
    }
}