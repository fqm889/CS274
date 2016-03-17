package Operations;

/**
 * Created by sicongfeng on 16/2/19.
 */
public class AbortOp extends Operation {
    @Override
    public Status doOp() {
        return Status.OK;
    }

    @Override
    public Status undoOP() {
        return Status.OK;
    }
}
