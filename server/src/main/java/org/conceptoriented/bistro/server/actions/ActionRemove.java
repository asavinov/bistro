package org.conceptoriented.bistro.server.actions;

import java.time.Duration;
import java.time.Instant;

import org.conceptoriented.bistro.core.*;
import org.conceptoriented.bistro.server.*;

/**
 * Remove record(s) from a table which do not fit into the window of the specified size.
 * Window size is either the number of records or duration.
 * If window size is 0 then all records will be deleted and the table will be emptied.
 */
public class ActionRemove implements Action {

    protected Table table;
    protected Column column;
    protected Duration age;
    protected long size;

    @Override
    public void eval(Context context) throws BistroError {
        if(this.table == null) return;

        if(this.column != null && this.age != null) {
            this.removeDuration();
        }
        else {
            this.removeCount();
        }
    }

    // Ensure that the table has count elements or less
    protected void removeCount() {
        long toRemove = table.getLength() - this.size;

        if(toRemove > 0) {
            this.table.remove(toRemove);
        }
    }

    // Ensure that the table has only elements with specified age or younger
    protected void removeDuration() {

        if(this.table.getLength() == 0) return;

        Range range = this.table.getIdRange();

        // We measure age relative to the youngest record
        Instant now = (Instant)this.column.getValue(range.end);
        // Alternatively, we could measure age relative to the current time
        // Instant now = Instant.now();

        // Iterate starting from the oldest elements and moving to the youngest element
        long toRemove = 0;
        for(long i=range.start; i<range.end; i++) {
            Instant t = (Instant)this.column.getValue(i);
            Duration d = Duration.between(t, now);

            if(this.age.compareTo(d) <= 0) { // If it is old, that is, its age is more than the specified
                toRemove++;
            }
            else {
                break;
            }
        }

        if(toRemove > 0) {
            this.table.remove(toRemove);
        }
    }

    // Remove last (oldest) elements so that the table has the specified number of elements
    public ActionRemove(Table table, long size) {
        this.table = table;
        this.size = size;
    }

    // Remove last (oldest) elements by ensuring that the table has only the elements of the specified age or younger
    public ActionRemove(Table table, Column column, Duration age) {
        this.table = table;
        this.column = column;
        this.age = age;
    }

}
