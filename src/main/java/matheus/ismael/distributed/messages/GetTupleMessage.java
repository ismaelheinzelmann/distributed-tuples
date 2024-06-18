package matheus.ismael.distributed.messages;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class GetTupleMessage implements Serializable {
    ArrayList<String> tuple;
}