package LSHFIndex;

import global.*;
import heap.*;
import java.io.*;
import java.util.*;

public class LSHFIndex implements Serializable {
    private int h; // Number of hash functions per layer
    private int L; // Number of layers
    private List<PrefixTree> layers; // List of prefix trees (one per layer)

    // Constructor
    public LSHFIndex(int h, int L) {
        this.h = h;
        this.L = L;
        this.layers = new ArrayList<>();
        int baseSeed = 12345; // Fixed base seed for reproducibility
        for (int i = 0; i < L; i++) {
            layers.add(new PrefixTree(baseSeed + i, h)); // Unique seed for each layer
        }
    }

    // Insert a key-RID pair into the index
    public void insert(Vector100DKey key, RID rid) throws IOException {
        for (PrefixTree layer : layers) {
            Vector100Dtype vector = key.getVector();
            int hashValue = layer.computeHash(vector); // Delegate hash computation to PrefixTree
            layer.insert(hashValue, vector, rid);
        }
    }

    // Delete a key-RID pair from the index
    public void delete(Vector100DKey key, RID rid) throws IOException {
        Vector100Dtype vector = key.getVector();
        for (PrefixTree layer : layers) {
            int hashValue = layer.computeHash(vector); // Delegate hash computation to PrefixTree
            layer.delete(hashValue, vector, rid);
        }
    }

    // Perform a range search
    public List<RID> rangeSearch(Vector100DKey target, int distance) throws IOException {
        Vector100Dtype vector = target.getVector();  
        List<RID> results = new ArrayList<>();
        for (PrefixTree layer : layers) {
            results.addAll(layer.rangeSearch(vector, distance));
        }
        return results;
    }

    // Perform a nearest-neighbor search
    public List<RIDDistancePair> nearestNeighborSearch(Vector100DKey target, int k) throws IOException {
        Vector100Dtype vector = target.getVector();
        PriorityQueue<RIDDistancePair> pq = new PriorityQueue<>(Comparator.comparingDouble(p -> p.distance));
        for (PrefixTree layer : layers) {
            pq.addAll(layer.nearestNeighborSearch(vector, k)); // Ensure PrefixTree returns RIDDistancePair
        }
        List<RIDDistancePair> results = new ArrayList<>();
        while (!pq.isEmpty() && results.size() < k) {
            results.add(pq.poll());
        }
        return results;
    }

    // Save the index to disk
    public void saveIndex(String filePath) throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeObject(this);
        }
    }

    // Load the index from disk
    public static LSHFIndex loadIndex(String filePath) throws IOException, ClassNotFoundException {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            return (LSHFIndex) ois.readObject();
        }
    }
}