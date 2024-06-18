package matheus.ismael.distributed.messages;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jgroups.Address;
import org.jgroups.util.Tuple;

import java.io.Serializable;
@Data
@AllArgsConstructor
@NoArgsConstructor
public class GetTupleQueueMessage implements Serializable {
    private String pattern;
    private String address;
}
