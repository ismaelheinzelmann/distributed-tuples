package matheus.ismael.distributed;


import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;

public class App {
    public static void main(String[] args) throws Exception {
        DistributedTupleSpaces distributedTupleSpaces = new DistributedTupleSpaces();
        Scanner reader = new Scanner(System.in);
        System.out.println("Distributed Tuple Space");
        System.out.println("Usage:");
        System.out.println("\tValues separated by a ','");
        System.out.println("\t* is a wildcard");
        while (true){
            System.out.println();
            System.out.println("Options:");
            for (Option option : Option.values()) {
                System.out.println("-> " + option);
            }
            String input = reader.nextLine();
            Optional<Option> option = Option.findByCode(input);
            if (option.isPresent()) {
                switch (option.get()) {
                    case get -> {
                        String tupleInput = reader.nextLine();
                        ArrayList<String> test = distributedTupleSpaces.get(List.of(tupleInput.split(",")));
                        System.out.println(test);
                    }
                    case write -> {
                        String tupleInput = reader.nextLine();
                        distributedTupleSpaces.write(List.of(tupleInput.split(",")));
                    }
                    case read -> {
                        System.out.println("read");
                    }
                    case list -> {
                        distributedTupleSpaces.list();
                    }
                    case exit -> {
                        distributedTupleSpaces.close();
                        System.exit(0);
                    }
                }
            }
        }
    }

    private void storeTuple(String tuple){

    }
}
