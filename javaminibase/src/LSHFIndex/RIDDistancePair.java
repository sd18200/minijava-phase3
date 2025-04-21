package LSHFIndex;

import global.RID;

public class RIDDistancePair {
    public RID rid;
    public double distance;

    public RIDDistancePair(RID rid, double distance) {
        this.rid = rid;
        this.distance = distance;
    }
}