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
        PriorityQueue<RIDDistancePair> pq = new PriorityQueue<>(
            Comparator.comparingDouble(p -> -p.distance)); // Max heap
        
        int targetHash = computeHash(target);
        
        // First pass: collect all candidates using hamming distance as filter
        for (Map.Entry<Integer, Map<Vector100Dtype, List<RID>>> entry : tree.entrySet()) {
            int hashValue = entry.getKey();
            // Use hamming distance as a quick filter
            if (hammingDistance(hashValue, targetHash) <= h/2) {
                for (Map.Entry<Vector100Dtype, List<RID>> vectorEntry : entry.getValue().entrySet()) {
                    Vector100Dtype vector = vectorEntry.getKey();
                    // Calculate actual distance between vectors
                    double actualDistance = vector.distanceTo(target);
                    
                    for (RID rid : vectorEntry.getValue()) {
                        if (pq.size() < k) {
                            // Add to results if we don't have k elements yet
                            pq.add(new RIDDistancePair(rid, actualDistance));
                        } else if (actualDistance < -pq.peek().distance) {
                            // Replace max element if we found a closer one
                            pq.poll();
                            pq.add(new RIDDistancePair(rid, actualDistance));
                        }
                    }
                }
            }
        }
        
        // Convert priority queue to sorted list
        List<RIDDistancePair> results = new ArrayList<>();
        RIDDistancePair pair;
        while ((pair = pq.poll()) != null) {
            // Fix the negated distance
            pair.distance = -pair.distance;
            results.add(0, pair); // Add at front to reverse order
        }
        
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