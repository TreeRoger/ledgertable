package dev.ledgertable.log;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * One entry of the transaction log, stored as {@code _log/<version>.json}. Writing it with
 * put-if-absent is the commit point: whichever writer creates the object owns that version.
 *
 * @param txnId       unique per logical transaction, so a writer can recognise its own commit after an ambiguous write
 * @param readVersion the version the writer originally read; the commit was rebased over everything in between
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Commit(long version, String txnId, long timestampMillis, Operation operation, long readVersion,
                     List<AddFile> adds, List<String> removes, ProducerMark producer) {
    public Commit {
        adds = List.copyOf(adds);
        removes = List.copyOf(removes);
    }
}
