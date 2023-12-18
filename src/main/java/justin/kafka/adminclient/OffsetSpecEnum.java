package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.OffsetSpec;

public enum OffsetSpecEnum {
    Latest(OffsetSpec.latest()),
    Earliest(OffsetSpec.earliest()),
    MaxTimeStamp(OffsetSpec.maxTimestamp())
    ;

    private final OffsetSpec offsetSpec;

    OffsetSpecEnum(OffsetSpec offsetSpec) {
        this.offsetSpec = offsetSpec;
    }

    public OffsetSpec getOffsetSpec() {
        return offsetSpec;
    }
}

