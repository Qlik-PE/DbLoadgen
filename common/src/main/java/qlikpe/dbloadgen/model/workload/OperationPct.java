
package qlikpe.dbloadgen.model.workload;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Specifies the what the distribution of INSERT/UPDATE/DELETE
 * operations should be.
 */
@Getter
@Setter
@NoArgsConstructor
public class OperationPct {
    private int insert;
    private int update;
    private int delete;

    public OperationPct(int insert, int update, int delete) {
        this.insert = insert;
        this.update = update;
        this.delete = delete;
    }
}
