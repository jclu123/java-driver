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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstracts the details of batching a sequence of {@link Message.Request}s into one or more {@link
 * Segment}s before sending them out on the network.
 *
 * <p>This class is not thread-safe.
 */
abstract class SegmentBuilder implements Connection.RequestWriter {

  private static final Logger logger = LoggerFactory.getLogger(SegmentBuilder.class);

  private static final int INITIAL_PAYLOAD_LENGTH = 4 * 1024;

  private final ByteBufAllocator allocator;
  private final int maxPayloadLength;
  private final Message.ProtocolEncoder requestEncoder;

  private ByteBuf currentPayload;
  private List<ChannelFutureListener> currentWriteListeners;

  SegmentBuilder(ByteBufAllocator allocator, Message.ProtocolEncoder requestEncoder) {
    this(allocator, requestEncoder, Segment.MAX_PAYLOAD_LENGTH);
  }

  /** Exposes the max length for unit tests; in production, this is hard-coded. */
  @VisibleForTesting
  SegmentBuilder(
      ByteBufAllocator allocator, Message.ProtocolEncoder requestEncoder, int maxPayloadLength) {
    this.allocator = allocator;
    this.requestEncoder = requestEncoder;
    this.maxPayloadLength = maxPayloadLength;

    resetCurrentPayload();
  }

  /** What to do whenever a full segment is ready. */
  abstract void processSegment(Segment segment);

  @Override
  public void addRequest(Message.Request request, ChannelFutureListener writeListener) {

    // Wrap the request into a legacy frame, append that frame to the payload.
    int frameHeaderLength = Frame.Header.lengthFor(requestEncoder.protocolVersion);
    int frameBodyLength = requestEncoder.encodedSize(request);
    int frameLength = frameHeaderLength + frameBodyLength;

    Frame.Header header =
        new Frame.Header(
            requestEncoder.protocolVersion,
            requestEncoder.computeFlags(request),
            request.getStreamId(),
            request.type.opcode,
            frameBodyLength);

    if (frameLength > maxPayloadLength) {
      // Large request: split into multiple dedicated segments and process them immediately:
      ByteBuf frame = allocator.ioBuffer(frameLength);
      header.encodeInto(frame);
      requestEncoder.encode(request, frame);

      int sliceCount =
          (frameLength / maxPayloadLength) + (frameLength % maxPayloadLength == 0 ? 0 : 1);
      SliceWriteListener sliceListener = new SliceWriteListener(sliceCount, writeListener);

      logger.trace(
          "Splitting large request ({} bytes) into {} segments: {}",
          frameLength,
          sliceCount,
          request);

      do {
        ByteBuf part = frame.readSlice(Math.min(maxPayloadLength, frame.readableBytes()));
        part.retain();
        process(part, false, Collections.<ChannelFutureListener>singletonList(sliceListener));
      } while (frame.isReadable());
      // We've retained each slice, and won't reference this buffer anymore
      frame.release();
    } else {
      // Small request: append to an existing segment, together with other messages.
      if (currentPayload.readableBytes() + frameLength > maxPayloadLength) {
        // Current segment is full, process and start a new one:
        processCurrentPayload();
        resetCurrentPayload();
      }
      // Append frame to current segment
      logger.trace(
          "Adding {}th request to self-contained segment: {}",
          currentWriteListeners.size() + 1,
          request);
      header.encodeInto(currentPayload);
      requestEncoder.encode(request, currentPayload);
      currentWriteListeners.add(writeListener);
    }
  }

  @Override
  public void flush() {
    if (currentPayload.readableBytes() > 0) {
      processCurrentPayload();
      resetCurrentPayload();
    }
  }

  private void process(
      ByteBuf payload, boolean isSelfContained, List<ChannelFutureListener> writeListeners) {
    processSegment(Segment.outgoing(payload, isSelfContained, writeListeners));
  }

  private void processCurrentPayload() {
    logger.trace(
        "Emitting new self-contained segment with {} frame(s)", currentWriteListeners.size());
    process(currentPayload, true, currentWriteListeners);
  }

  private void resetCurrentPayload() {
    currentPayload = this.allocator.ioBuffer(INITIAL_PAYLOAD_LENGTH, Segment.MAX_PAYLOAD_LENGTH);
    currentWriteListeners = new ArrayList<ChannelFutureListener>();
  }

  /** Notifies the original listener when all slices of a message have been written. */
  private static class SliceWriteListener implements ChannelFutureListener {

    // All slices are written to the same channel, and the segment is built from the Flusher which
    // also runs on the same event loop, so we don't need synchronization.
    private int remainingSlices;
    private boolean alreadyFailed;

    private final ChannelFutureListener parentListener;

    public SliceWriteListener(int remainingSlices, ChannelFutureListener parentListener) {
      this.remainingSlices = remainingSlices;
      this.parentListener = parentListener;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      if (!alreadyFailed) {
        if (future.isSuccess()) {
          remainingSlices -= 1;
          if (remainingSlices == 0) {
            parentListener.operationComplete(future);
          }
        } else {
          // No need to wait for the outcome of the other slices really. Fail immediately and we'll
          // ignore them.
          parentListener.operationComplete(future);
          alreadyFailed = true;
        }
      }
    }
  }
}
