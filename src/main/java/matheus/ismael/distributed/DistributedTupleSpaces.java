package matheus.ismael.distributed;

import lombok.SneakyThrows;
import matheus.ismael.distributed.messages.*;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.blocks.locking.LockService;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Tuple;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DistributedTupleSpaces {
    private final JChannel channel;
    private final ArrayList<ArrayList<String>> tupleSpace = new ArrayList<>();
    private final LockService lockService;

    private final Lock tupleSpaceLock = new ReentrantLock();
    private final Condition tupleSpaceCondition;
    private final TupleReceiver tupleReceiver;
    private final ArrayList<Tuple<String, Address>> getQueue = new ArrayList<>();

    DistributedTupleSpaces() throws Exception {
        InputStream inputStream = TupleReceiver.class.getClassLoader().getResourceAsStream("udp.xml");
        if (inputStream == null) {
            throw new RuntimeException("udp.xml not found in resources");
        }
        channel = new JChannel(inputStream);
        channel.connect("tuple-spaces");
        channel.setDiscardOwnMessages(true);
        lockService = new LockService(channel);
        tupleSpaceCondition = tupleSpaceLock.newCondition();
        tupleReceiver = new TupleReceiver(channel, tupleSpace, lockService, tupleSpaceLock, tupleSpaceCondition,
                getQueue);
        channel.setReceiver(tupleReceiver);
        Thread thread = new Thread(tupleReceiver);
        thread.start();
        if (channel.getView().getCoord() != channel.getAddress()) {
            sendGetStateRequest();
        }
    }

    public Optional<ArrayList<String>> read(String tuplePattern){
        return tupleReceiver.read(tuplePattern);
    }

    public void write(List<String> tuple) throws Exception {
        Lock lock = lockService.getLock("tuple-spaces");
        lock.lock();
        try {
            synchronized (tupleSpaceLock) {
                String pattern = String.join(",", tuple);

                var tupleInSpace = read(pattern);
                if (tupleInSpace.isEmpty()){
                    var queuedGetRequest = tupleReceiver.getMatchingPattern(pattern);
                    if (queuedGetRequest.isPresent()){
                        channel.send(new ObjectMessage(queuedGetRequest.get().getVal2(), new WriteTupleMessage(new ArrayList<>(tuple))));

                        //implement the send of the tuple message, the receive of the message in the requester, and the requester sending the successful for removal of the queues
                    } else {
                        tupleSpace.add(new ArrayList<>(tuple));
                        sendMessage(new WriteTupleMessage(new ArrayList<>(tuple)));
                    }

                }
            }
        } finally {
            lock.unlock();
        }
    }

    public ArrayList<String> get(List<String> tuple) throws Exception {
        Lock lock = lockService.getLock("tuple-spaces");
        lock.lock();
        try {
            Optional<ArrayList<String>> result;
            tupleSpaceLock.lock();
            do {
                String pattern = String.join(",", tuple);
                result = read(pattern);
                if (result.isEmpty()) {
                    tupleReceiver.setPattern(pattern);
                    channel.send(new ObjectMessage(null, new GetTupleQueueMessage(pattern, channel.getAddress().toString())));
                    lock.unlock();
                    tupleSpaceCondition.await();
                    lock.lock();
                    channel.send(new ObjectMessage(null, new GetTupleQueueRemovalMessage()));
                    tupleReceiver.setPattern(null);
                }
            } while (result.isEmpty());

            tupleSpace.remove(result.get());
            sendMessage(new GetTupleMessage(new ArrayList<>(tuple)));
            tupleSpaceLock.unlock();
            return result.get();
        } finally {
            lock.unlock();
        }
    }

    private void sendGetStateRequest() throws Exception {
        Message message = new ObjectMessage(channel.getView().getCoord(), new GetStateMessage());
        channel.send(message);
    }

    private void sendMessage(Object messageObject) throws Exception {
        Message message = new ObjectMessage(null, messageObject);
        channel.send(message);
    }

    public void close() {
        channel.close();
    }

    public void list() {
        for (List<String> tuple : tupleSpace) {
            System.out.println(tuple);
        }
    }
}
