/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class SegmentBuilderTest {

  private static final Message.ProtocolEncoder REQUEST_ENCODER =
      new Message.ProtocolEncoder(ProtocolVersion.V5);

  // The constant names denote the total encoded size, including the frame header
  private static final Message.Request _38B_REQUEST = new Requests.Query("SELECT * FROM table");
  private static final Message.Request _51B_REQUEST =
      new Requests.Query("SELECT * FROM table WHERE id = 1");
  private static final Message.Request _1KB_REQUEST =
      new Requests.Query(
          "SELECT * FROM table WHERE id = ?",
          new Requests.QueryProtocolOptions(
              Message.Request.Type.QUERY,
              ConsistencyLevel.ONE,
              new ByteBuffer[] {ByteBuffer.allocate(967)},
              Collections.<String, ByteBuffer>emptyMap(),
              false,
              -1,
              null,
              ConsistencyLevel.SERIAL,
              Long.MIN_VALUE,
              Integer.MIN_VALUE),
          false);

  // We need this to build promises in the tests
  private static final Channel MOCK_CHANNEL = new EmbeddedChannel();

  @Test(groups = "unit")
  public void should_concatenate_frames_when_under_limit() {
    TestSegmentBuilder builder = new TestSegmentBuilder(100);

    builder.addRequest(_38B_REQUEST, mockWriteListener());
    builder.addRequest(_51B_REQUEST, mockWriteListener());
    // Nothing produced yet since we would still have room for more frames
    assertThat(builder.segments).isEmpty();

    builder.flush();
    assertThat(builder.segments).hasSize(1);
    Segment segment = builder.segments.get(0);
    assertThat(segment.getPayload().readableBytes()).isEqualTo(38 + 51);
    assertThat(segment.isSelfContained()).isTrue();
    assertThat(segment.getWriteListeners()).hasSize(2);
  }

  @Test(groups = "unit")
  public void should_start_new_segment_when_over_limit() {
    TestSegmentBuilder builder = new TestSegmentBuilder(100);

    builder.addRequest(_38B_REQUEST, mockWriteListener());
    builder.addRequest(_51B_REQUEST, mockWriteListener());
    builder.addRequest(_38B_REQUEST, mockWriteListener());
    // Adding the 3rd frame brings the total size over 100, so a first segment should be emitted
    // with the first two messages:
    assertThat(builder.segments).hasSize(1);

    builder.addRequest(_38B_REQUEST, mockWriteListener());
    builder.flush();
    assertThat(builder.segments).hasSize(2);

    Segment segment1 = builder.segments.get(0);
    assertThat(segment1.getPayload().readableBytes()).isEqualTo(38 + 51);
    assertThat(segment1.isSelfContained()).isTrue();
    assertThat(segment1.getWriteListeners()).hasSize(2);
    Segment segment2 = builder.segments.get(1);
    assertThat(segment2.getPayload().readableBytes()).isEqualTo(38 + 38);
    assertThat(segment2.isSelfContained()).isTrue();
    assertThat(segment2.getWriteListeners()).hasSize(2);
  }

  @Test(groups = "unit")
  public void should_start_new_segment_when_at_limit() {
    TestSegmentBuilder builder = new TestSegmentBuilder(38 + 51);

    builder.addRequest(_38B_REQUEST, mockWriteListener());
    builder.addRequest(_51B_REQUEST, mockWriteListener());
    builder.addRequest(_38B_REQUEST, mockWriteListener());
    assertThat(builder.segments).hasSize(1);

    builder.addRequest(_51B_REQUEST, mockWriteListener());
    builder.flush();
    assertThat(builder.segments).hasSize(2);

    Segment segment1 = builder.segments.get(0);
    assertThat(segment1.getPayload().readableBytes()).isEqualTo(38 + 51);
    assertThat(segment1.isSelfContained()).isTrue();
    assertThat(segment1.getWriteListeners()).hasSize(2);
    Segment segment2 = builder.segments.get(1);
    assertThat(segment2.getPayload().readableBytes()).isEqualTo(38 + 51);
    assertThat(segment2.isSelfContained()).isTrue();
    assertThat(segment2.getWriteListeners()).hasSize(2);
  }

  @Test(groups = "unit")
  public void should_split_large_frame() throws Exception {
    TestSegmentBuilder builder = new TestSegmentBuilder(100);

    ChannelFutureListener parentListener = mockWriteListener();
    builder.addRequest(_1KB_REQUEST, parentListener);

    assertThat(builder.segments).hasSize(11);
    for (int i = 0; i < 11; i++) {
      Segment slice = builder.segments.get(i);
      assertThat(slice.getPayload().readableBytes()).isEqualTo(i == 10 ? 24 : 100);
      assertThat(slice.isSelfContained()).isFalse();
      assertThat(slice.getWriteListeners()).hasSize(1);
    }
  }

  @Test(groups = "unit")
  public void should_succeed_parent_write_if_all_slices_successful() throws Exception {
    TestSegmentBuilder builder = new TestSegmentBuilder(100);

    ChannelFutureListener parentListener = mockWriteListener();
    builder.addRequest(_1KB_REQUEST, parentListener);

    assertThat(builder.segments).hasSize(11);

    for (int i = 0; i < 10; i++) {
      Segment slice = builder.segments.get(i);
      slice.getWriteListeners().get(0).operationComplete(mockSuccessfulFuture());
      verify(parentListener, never()).operationComplete(any(ChannelFuture.class));
    }

    Segment lastSlice = builder.segments.get(10);
    ChannelFuture lastFuture = mockSuccessfulFuture();
    lastSlice.getWriteListeners().get(0).operationComplete(lastFuture);
    verify(parentListener).operationComplete(lastFuture);
  }

  @Test(groups = "unit")
  public void should_fail_parent_write_if_any_slice_fails() throws Exception {
    TestSegmentBuilder builder = new TestSegmentBuilder(100);

    ChannelFutureListener parentListener = mockWriteListener();
    builder.addRequest(_1KB_REQUEST, parentListener);

    assertThat(builder.segments).hasSize(11);

    for (int i = 0; i < 5; i++) {
      Segment slice = builder.segments.get(i);
      slice.getWriteListeners().get(0).operationComplete(mockSuccessfulFuture());
      verify(parentListener, never()).operationComplete(any(ChannelFuture.class));
    }

    ChannelFuture failedFuture = mockFailedFuture();
    Segment failingSlice = builder.segments.get(5);
    failingSlice.getWriteListeners().get(0).operationComplete(failedFuture);
    verify(parentListener).operationComplete(failedFuture);

    for (int i = 6; i < 11; i++) {
      Segment slice = builder.segments.get(i);
      slice.getWriteListeners().get(0).operationComplete(mockSuccessfulFuture());
    }
    verifyNoMoreInteractions(parentListener);
  }

  @Test(groups = "unit")
  public void should_split_large_frame_when_exact_multiple() {
    TestSegmentBuilder builder = new TestSegmentBuilder(256);

    ChannelFutureListener parentListener = mockWriteListener();
    builder.addRequest(_1KB_REQUEST, parentListener);

    assertThat(builder.segments).hasSize(4);
    for (int i = 0; i < 4; i++) {
      Segment slice = builder.segments.get(i);
      assertThat(slice.getPayload().readableBytes()).isEqualTo(256);
      assertThat(slice.isSelfContained()).isFalse();
      assertThat(slice.getWriteListeners()).hasSize(1);
    }
  }

  @Test(groups = "unit")
  public void should_mix_small_frames_and_large_frames() {
    TestSegmentBuilder builder = new TestSegmentBuilder(100);

    builder.addRequest(_38B_REQUEST, mockWriteListener());
    builder.addRequest(_51B_REQUEST, mockWriteListener());

    // Large frame: process immediately, does not impact accumulated small frames
    builder.addRequest(_1KB_REQUEST, mockWriteListener());
    assertThat(builder.segments).hasSize(11);

    // Another small frames bring us above the limit
    builder.addRequest(_38B_REQUEST, mockWriteListener());
    assertThat(builder.segments).hasSize(12);

    // One last frame and finish
    builder.addRequest(_38B_REQUEST, mockWriteListener());
    builder.flush();
    assertThat(builder.segments).hasSize(13);

    for (int i = 0; i < 11; i++) {
      Segment slice = builder.segments.get(i);
      assertThat(slice.getPayload().readableBytes()).isEqualTo(i == 10 ? 24 : 100);
      assertThat(slice.isSelfContained()).isFalse();
    }

    Segment smallMessages1 = builder.segments.get(11);
    assertThat(smallMessages1.getPayload().readableBytes()).isEqualTo(38 + 51);
    assertThat(smallMessages1.isSelfContained()).isTrue();
    Segment smallMessages2 = builder.segments.get(12);
    assertThat(smallMessages2.getPayload().readableBytes()).isEqualTo(38 + 38);
    assertThat(smallMessages2.isSelfContained()).isTrue();
  }

  private static ChannelFutureListener mockWriteListener() {
    return mock(ChannelFutureListener.class);
  }

  private static ChannelFuture mockSuccessfulFuture() {
    DefaultChannelPromise future = new DefaultChannelPromise(MOCK_CHANNEL);
    future.setSuccess();
    return future;
  }

  private static ChannelFuture mockFailedFuture() {
    DefaultChannelPromise future = new DefaultChannelPromise(MOCK_CHANNEL);
    future.setFailure(new RuntimeException("mock failure"));
    return future;
  }

  // Test implementation that simply stores segment in the order they were produced.
  static class TestSegmentBuilder extends SegmentBuilder {

    List<Segment> segments = new ArrayList<Segment>();

    TestSegmentBuilder(int maxPayloadLength) {
      super(ByteBufAllocator.DEFAULT, REQUEST_ENCODER, maxPayloadLength);
    }

    @Override
    void processSegment(Segment segment) {
      segments.add(segment);
    }
  }
}
