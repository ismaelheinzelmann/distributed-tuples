package matheus.ismael.distributed;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

public enum Option {
    get,
    write,
    read,
    list,
    exit;

    public static Optional<Option> findByCode(String value) {
        return Arrays.stream(values()).filter(v -> Objects.equals(v.toString(), value)).findFirst();
    }
}