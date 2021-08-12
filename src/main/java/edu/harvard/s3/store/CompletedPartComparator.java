package edu.harvard.s3.store;

import java.util.Comparator;
import software.amazon.awssdk.services.s3.model.CompletedPart;

/**
 * Completed part comparator ordering by part number.
 */
public class CompletedPartComparator implements Comparator<CompletedPart> {

    @Override
    public int compare(CompletedPart cp1, CompletedPart cp2) {
        return cp1.partNumber().compareTo(cp2.partNumber());
    }

}
