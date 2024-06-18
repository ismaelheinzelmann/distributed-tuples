package matheus.ismael.distributed;

import lombok.SneakyThrows;
import matheus.ismael.distributed.messages.*;
import org.jgroups.*;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Tuple;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

//QUANDO QUISER DAR UM GET, BROADCAST UMA MENSAGEM FALANDO QUERO TAL PADRAO, ONDE CADA INSTANCIA VAI TER UMA
//LISTA DE SOLICITAÇOES, QUE CASO SUA REQUISIÇAO DE WRITE DE MATCH COM ALGUMA, ENVIA DIRETAMENTE PARA A INSTANCIA QUE ESTARA ESPERANDO E ESPERARA UM SUCESS
public class TupleReceiver implements Receiver, Runnable {
    private final JChannel channel;
    private final ArrayList<ArrayList<String>> tupleSpace;
    private final LockService lockService;
    private final ArrayList<Tuple<String, Address>> getQueue; //Pattern, address

    private final Lock tupleSpaceLock;
    private final Condition tupleSpaceCondition;
    private String pattern = null;
    private Address stateRequesterAddress = null;

    TupleReceiver(JChannel channel,
                  ArrayList<ArrayList<String>> tupleSpace,
                  LockService lockService,
                  Lock tupleSpaceLock,
                  Condition tupleSpaceCondition, ArrayList<Tuple<String, Address>> getQueue) {
        this.channel = channel;
        this.tupleSpace = tupleSpace;
        this.lockService = lockService;
        this.tupleSpaceLock = tupleSpaceLock;
        this.tupleSpaceCondition = tupleSpaceCondition;
        this.getQueue = getQueue;
    }

    void setPattern(String pattern) {
        this.pattern = pattern;
    }

    @SneakyThrows
    @Override
    public void receive(MessageBatch batch) {
        Object message = batch.stream().iterator().next().getObject();
        if (message instanceof WriteTupleMessage) {
            receiveWrite((WriteTupleMessage) message);
        } else if (message instanceof GetTupleMessage) {
            receiveGet((GetTupleMessage) message);
        } else if (message instanceof StateMessage) {
            receiveState((StateMessage) message, batch.getSender());
        } else if (message instanceof GetStateMessage) {
            sendState(batch.getSender());
        } else if (message instanceof StateSuccessMessage && batch.getSender().equals(stateRequesterAddress)) {
            tupleSpaceLock.lock();
            try {
                tupleSpaceCondition.signalAll();
            } finally {
                tupleSpaceLock.unlock();
            }
        } else if (message instanceof GetTupleQueueMessage){
            synchronized (getQueue){
                getQueue.add(new Tuple<>(((GetTupleQueueMessage) message).getPattern(), batch.getSender()));
            }
        } else if (message instanceof GetTupleQueueRemovalMessage){
            synchronized (getQueue){
                getQueue.removeIf(tuple -> tuple.getVal2().equals(batch.getSender()));
            }
        }
    }

    private void receiveState(StateMessage message, Address sender) throws Exception {
        tupleSpaceLock.lock();
        try {
            tupleSpace.clear();
            tupleSpace.addAll(message.getTuples());
            Message destinMessage = new ObjectMessage(sender, new StateSuccessMessage());
            channel.send(destinMessage);
        } finally {
            tupleSpaceLock.unlock();
        }
    }

    private void sendState(Address address) throws Exception {
        Lock get = lockService.getLock("tuple-spaces");
        try {
            tupleSpaceLock.lock();
            try {
                Message stateMessage = new ObjectMessage(address, new StateMessage(tupleSpace));
                stateRequesterAddress = address;
                channel.send(stateMessage);
                tupleSpaceCondition.await(1500, TimeUnit.MILLISECONDS);
            } finally {
                tupleSpaceLock.unlock();
            }
        } finally {
            get.unlock();
        }
    }

    private void receiveWrite(WriteTupleMessage writeTupleMessage) {
        tupleSpaceLock.lock();
        try {
            var tupleInSpace = read(String.join(",", writeTupleMessage.getTuple()));
            if (tupleInSpace.isEmpty()) {
                tupleSpace.add(writeTupleMessage.getTuple());
                if (pattern != null && !pattern.isEmpty()) {
                    Lock lock = lockService.getLock("tuple-spaces");
                    var test = read(pattern);
                    if (test.isPresent()) {
                        tupleSpaceCondition.signalAll();
                    }
                    lock.unlock();
                }
            }
        } finally {
            tupleSpaceLock.unlock();
        }
    }

    private void receiveGet(GetTupleMessage getTupleMessage) {
        tupleSpaceLock.lock();
        try {
            var tupleInSpace = read(String.join(",", getTupleMessage.getTuple()));
            tupleInSpace.ifPresent(tupleSpace::remove);
        } finally {
            tupleSpaceLock.unlock();
        }
    }

    Optional<ArrayList<String>> read(String tuplePattern) {
        String[] pattern = tuplePattern.split(",");
        for (ArrayList<String> tuple : tupleSpace) {
            if (pattern.length != tuple.size()) {
                continue;
            }
            boolean verify = true;
            for (int j = 0; j < pattern.length; j++) {
                if (pattern[j].endsWith("*") && pattern[j].length() == 1) {
                    continue;
                }
                if (!Objects.equals(pattern[j], tuple.get(j))) {
                    verify = false;
                    break;
                }
            }
            if (verify) {
                return Optional.of(tuple);
            }
        }
        return Optional.empty();
    }

    public Optional<Tuple<String, Address>> getMatchingPattern(String tuplePattern){
        String[] pattern = tuplePattern.split(",");
        for (Tuple<String, Address> tuple : getQueue) {
            String[] getQueuePattern = tuple.getVal1().split(",");
            if (pattern.length != getQueuePattern.length) {
                continue;
            }
            boolean verify = true;
            for (int j = 0; j < pattern.length; j++) {
                if (pattern[j].endsWith("*") && pattern[j].length() == 1) {
                    continue;
                }
                if (!Objects.equals(pattern[j], getQueuePattern[j])) {
                    verify = false;
                    break;
                }
            }
            if (verify) {
                return Optional.of(tuple);
            }
        }
        return Optional.empty();
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
