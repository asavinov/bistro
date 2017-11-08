package org.conceptoriented.bistro.core;

public class Range {

    public long start;
    public long end;

	public long getLength() {
		return end - start;
	}
	
	public boolean isIn(long row) {
		if(row >= start && row < end) return true;
		return false;
	}

	public boolean isIn(Range r) { // Whether this range is completely covered by (is within) the specified range
		if(this.start < r.start) return false;
		if(this.end > r.end) return false;
		return true;
	}

	public Range uniteWith(Range r) {
		r.start = Long.min(this.start, r.start);
		r.end = Long.max(this.end, r.end);
		return r;
	}

	public Range addToEndOf(Range r) { // Add this range to the end of the specified range (by changing the argument). The argument can only become larger.
		if(this.end > r.end) r.end = this.end;
		return r;
	}

	public Range delFromStartOf(Range r) { // Remove this range from the start of the specified range (by changing the argument)
		if(this.end > r.start) r.start = this.end;
		if(r.start > r.end) r.end = r.start; // In the case the whole range is removed
		return r;
	}

    public static int[] intersect(int[] source, int[] target) { // Restrict the source array by elements from the second array
        int size=0;
        int[] result = new int[Math.min(source.length, target.length)];
        int trgFirst = 0;
        for(int src=0; src<source.length; src++) {

            for(int trg=trgFirst; trg<target.length; trg++) {
                if(source[src] != target[trg]) continue;
                // Found target in source
                result[size] = source[src]; // Store in the result
                size = size + 1;
                trgFirst = trg + 1;
                break;
            }
        }

        return java.util.Arrays.copyOf(result, size);
    }

    @Override
    public String toString() {
      return String.format("[%s, %s)", start, end);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (!(other instanceof Range)){
            return false;
        }

        Range other_ = (Range)other;

        // this may cause NPE if nulls are valid valuePaths for x or y. The logic may be improved to handle nulls properly, if needed.
        return other_.start == this.start && other_.end == this.end;
    }

    @Override
    public int hashCode() {
    	return Long.hashCode(start) ^ Long.hashCode(end);
    }

    public Range(Range range) {
        super();
        this.start = range.start;
        this.end = range.end;
      }

    public Range(long start, long end) {
        super();
        this.start = start;
        this.end = end;
      }

    public Range() {
        this(0,0);
      }
}
