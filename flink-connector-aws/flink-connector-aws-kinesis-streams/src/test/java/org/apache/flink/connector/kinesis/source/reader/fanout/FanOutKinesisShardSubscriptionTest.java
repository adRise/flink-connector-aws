/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.source.reader.fanout;

import org.apache.flink.connector.kinesis.source.proxy.AsyncStreamProxy;
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEvent;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardEventStream;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponse;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the event hand-off of {@link FanOutKinesisShardSubscription}.
 *
 * <p>The events of a stream are delivered on a Netty event loop that serves many shards. Blocking
 * it, which the subscription used to do when its queue was full and nobody polled, stops every
 * other shard on that loop and the responses to the subscribe calls that would let the reader poll
 * again.
 */
class FanOutKinesisShardSubscriptionTest {

    private static final String CONSUMER_ARN =
            "arn:aws:kinesis:us-west-2:123456789012:stream/s/consumer/c:1";
    private static final String SHARD_ID = "shardId-000000000000";

    /** One call of {@code subscribeToShard}: the test plays the Kinesis side of it. */
    private static final class Stream {
        final SubscribeToShardResponseHandler handler;
        final AtomicLong requested = new AtomicLong();
        final AtomicBoolean cancelled = new AtomicBoolean();
        volatile Subscriber<? super SubscribeToShardEventStream> subscriber;

        Stream(SubscribeToShardResponseHandler handler) {
            this.handler = handler;
        }

        /** The response arrives: the subscriber learns its subscription. */
        void connect() {
            handler.responseReceived(SubscribeToShardResponse.builder().build());
            // The lambda is the SdkPublisher; the handler subscribes the stream's subscriber to it.
            handler.onEventStream(
                    streamSubscriber -> {
                        subscriber = streamSubscriber;
                        streamSubscriber.onSubscribe(
                                new Subscription() {
                                    @Override
                                    public void request(long n) {
                                        requested.addAndGet(n);
                                    }

                                    @Override
                                    public void cancel() {
                                        cancelled.set(true);
                                    }
                                });
                    });
        }

        void deliver(String continuationSequenceNumber) {
            subscriber.onNext(event(continuationSequenceNumber));
        }
    }

    /** Records every subscribe call; connects each stream at once unless told to hold it. */
    private static final class ControlledProxy implements AsyncStreamProxy {
        final List<Stream> streams = new CopyOnWriteArrayList<>();
        volatile boolean connectAtOnce = true;

        @Override
        public CompletableFuture<Void> subscribeToShard(
                String consumerArn,
                String shardId,
                StartingPosition startingPosition,
                SubscribeToShardResponseHandler responseHandler) {
            Stream stream = new Stream(responseHandler);
            streams.add(stream);
            if (connectAtOnce) {
                stream.connect();
            }
            // never completes: the stream stays open
            return new CompletableFuture<>();
        }

        @Override
        public void close() throws IOException {}
    }

    private static SubscribeToShardEvent event(String continuationSequenceNumber) {
        return SubscribeToShardEvent.builder()
                .continuationSequenceNumber(continuationSequenceNumber)
                .millisBehindLatest(0L)
                .build();
    }

    private static FanOutKinesisShardSubscription subscriptionOn(ControlledProxy proxy) {
        return new FanOutKinesisShardSubscription(
                proxy,
                CONSUMER_ARN,
                SHARD_ID,
                StartingPosition.fromStart(),
                Duration.ofSeconds(30));
    }

    private static void eventually(BooleanSupplier condition, String what) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for " + what);
            }
            Thread.sleep(5);
        }
    }

    private static String sequenceNumber(SubscribeToShardEvent event) {
        return event == null ? null : event.continuationSequenceNumber();
    }

    @Test
    void asksForTwoBatchesWhenSubscribed() throws Exception {
        ControlledProxy proxy = new ControlledProxy();
        FanOutKinesisShardSubscription subscription = subscriptionOn(proxy);

        subscription.activateSubscription();

        eventually(() -> proxy.streams.get(0).requested.get() == 2, "the first two requests");
    }

    @Test
    void asksForAReplacementOnlyWhenABatchIsConsumed() throws Exception {
        ControlledProxy proxy = new ControlledProxy();
        FanOutKinesisShardSubscription subscription = subscriptionOn(proxy);
        subscription.activateSubscription();
        eventually(() -> proxy.streams.get(0).requested.get() == 2, "the first two requests");
        Stream stream = proxy.streams.get(0);

        stream.deliver("1");
        stream.deliver("2");
        assertThat(stream.requested.get()).as("nothing consumed yet").isEqualTo(2);

        assertThat(sequenceNumber(subscription.nextEvent())).isEqualTo("1");
        assertThat(stream.requested.get()).isEqualTo(3);
        assertThat(sequenceNumber(subscription.nextEvent())).isEqualTo("2");
        assertThat(stream.requested.get()).isEqualTo(4);
        assertThat(subscription.nextEvent()).isNull();
        assertThat(stream.requested.get()).as("an empty poll asks for nothing").isEqualTo(4);
    }

    @Test
    void theThreadDeliveringEventsIsNeverBlockedByAnUnpolledQueue() throws Exception {
        ControlledProxy proxy = new ControlledProxy();
        FanOutKinesisShardSubscription subscription = subscriptionOn(proxy);
        subscription.activateSubscription();
        eventually(() -> proxy.streams.get(0).requested.get() == 2, "the first two requests");
        Stream stream = proxy.streams.get(0);

        // Nobody polls. The queue used to hold two events, and the third delivery parked
        // its thread.
        ExecutorService eventLoop = Executors.newSingleThreadExecutor();
        try {
            Future<?> delivered =
                    eventLoop.submit(
                            () -> {
                                for (int i = 1; i <= 10; i++) {
                                    stream.deliver(String.valueOf(i));
                                }
                            });
            delivered.get(5, TimeUnit.SECONDS);
        } finally {
            eventLoop.shutdownNow();
        }

        for (int i = 1; i <= 10; i++) {
            assertThat(sequenceNumber(subscription.nextEvent())).isEqualTo(String.valueOf(i));
        }
    }

    @Test
    void eventsQueuedBeforeTheSubscriptionWentInactiveAreStillDelivered() throws Exception {
        ControlledProxy proxy = new ControlledProxy();
        FanOutKinesisShardSubscription subscription = subscriptionOn(proxy);
        subscription.activateSubscription();
        eventually(() -> proxy.streams.get(0).requested.get() == 2, "the first two requests");
        Stream first = proxy.streams.get(0);
        first.deliver("1");
        first.deliver("2");

        // The stream ends and the next one is not connected yet: the subscription is inactive.
        proxy.connectAtOnce = false;
        first.subscriber.onComplete();
        eventually(() -> proxy.streams.size() == 2, "the second subscribe call");

        assertThat(sequenceNumber(subscription.nextEvent())).isEqualTo("1");
        assertThat(sequenceNumber(subscription.nextEvent())).isEqualTo("2");
        assertThat(subscription.nextEvent()).isNull();
    }

    @Test
    void aReplacementStreamIsNotBlockedByEventsStillQueuedFromTheStreamBefore() throws Exception {
        ControlledProxy proxy = new ControlledProxy();
        FanOutKinesisShardSubscription subscription = subscriptionOn(proxy);
        subscription.activateSubscription();
        eventually(() -> proxy.streams.get(0).requested.get() == 2, "the first two requests");
        Stream first = proxy.streams.get(0);
        first.deliver("1");
        first.deliver("2");

        // The stream ends with two events still queued and nobody polling. The events of the
        // replacement stream must not wait for the reader either, even if more arrive than it
        // asked for (it asks for nothing: the queue is full).
        proxy.connectAtOnce = false;
        first.subscriber.onComplete();
        eventually(() -> proxy.streams.size() == 2, "the second subscribe call");
        proxy.streams.get(1).connect();
        ExecutorService eventLoop = Executors.newSingleThreadExecutor();
        try {
            Future<?> delivered =
                    eventLoop.submit(
                            () -> {
                                proxy.streams.get(1).deliver("3");
                                proxy.streams.get(1).deliver("4");
                                proxy.streams.get(1).deliver("5");
                            });
            delivered.get(5, TimeUnit.SECONDS);
        } finally {
            eventLoop.shutdownNow();
        }

        for (int i = 1; i <= 5; i++) {
            assertThat(sequenceNumber(subscription.nextEvent())).isEqualTo(String.valueOf(i));
        }
    }

    @Test
    void aReplacementStreamAsksOnlyForWhatTheQueueStillHasRoomFor() throws Exception {
        ControlledProxy proxy = new ControlledProxy();
        FanOutKinesisShardSubscription subscription = subscriptionOn(proxy);
        subscription.activateSubscription();
        eventually(() -> proxy.streams.get(0).requested.get() == 2, "the first two requests");
        proxy.streams.get(0).deliver("1");
        proxy.streams.get(0).deliver("2");

        // Nobody polls (the job is held up downstream) while the stream is replaced, which
        // happens every few minutes. The queue holds all the batches the subscription may keep:
        // a replacement that asked for two more every time would grow it for as long as the job
        // is held up.
        for (int i = 1; i <= 3; i++) {
            final int streams = i + 1;
            proxy.streams.get(i - 1).subscriber.onComplete();
            eventually(() -> proxy.streams.size() == streams, "the next subscribe call");
            // The subscription is marked active, and the requests are made, on another thread
            // once the stream has connected. A real stream lives for minutes before it ends.
            Thread.sleep(100);
        }
        Stream last = proxy.streams.get(3);
        assertThat(last.requested.get()).as("the queue is full").isEqualTo(0);

        assertThat(sequenceNumber(subscription.nextEvent())).isEqualTo("1");
        assertThat(last.requested.get()).isEqualTo(1);
        assertThat(sequenceNumber(subscription.nextEvent())).isEqualTo("2");
        assertThat(last.requested.get()).isEqualTo(2);
        assertThat(subscription.nextEvent()).isNull();
    }

    @Test
    void eventsOfAReplacedStreamAreDroppedAndItsSubscriptionCancelled() throws Exception {
        ControlledProxy proxy = new ControlledProxy();
        FanOutKinesisShardSubscription subscription = subscriptionOn(proxy);
        subscription.activateSubscription();
        eventually(() -> proxy.streams.get(0).requested.get() == 2, "the first two requests");
        Stream first = proxy.streams.get(0);
        first.deliver("1");

        first.subscriber.onComplete();
        eventually(() -> proxy.streams.size() == 2, "the second subscribe call");
        Stream second = proxy.streams.get(1);
        // one batch is still queued from the first stream, so there is room for one more
        eventually(() -> second.requested.get() == 1, "the second stream's request");

        // A late delivery of the first stream, then a normal one of the second.
        first.deliver("stale");
        second.deliver("2");

        assertThat(first.cancelled.get()).isTrue();
        assertThat(sequenceNumber(subscription.nextEvent())).isEqualTo("1");
        assertThat(sequenceNumber(subscription.nextEvent())).isEqualTo("2");
        assertThat(subscription.nextEvent()).isNull();
    }

    @Test
    void eachStreamGetsItsOwnSubscriber() throws Exception {
        ControlledProxy proxy = new ControlledProxy();
        proxy.connectAtOnce = false;
        FanOutKinesisShardSubscription subscription = subscriptionOn(proxy);

        // Two activations before the first stream connects (the first one timed out, say).
        subscription.activateSubscription();
        subscription.activateSubscription();
        eventually(() -> proxy.streams.size() == 2, "two subscribe calls");
        proxy.streams.get(0).connect();
        proxy.streams.get(1).connect();

        assertThat(proxy.streams.get(0).subscriber)
                .as("the earlier stream must not receive the later subscriber")
                .isNotSameAs(proxy.streams.get(1).subscriber);
    }
}
