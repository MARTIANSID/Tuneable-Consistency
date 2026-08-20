package org.example.Server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.example.raft.KvClientGrpc;
import org.junit.jupiter.api.Test;

import io.grpc.Metadata;

class ServerIngressOccupancyTracerTest {

    @Test
    void deframedRequestsCountTowardOccupancyUntilTheirCallbacksComplete() {
        MeasurementPlane plane = new MeasurementPlane(97, 3);
        try {
            ServerIngressOccupancyTracer tracer = (ServerIngressOccupancyTracer)
                    ServerIngressOccupancyTracer.factory(plane).newServerStreamTracer(
                            KvClientGrpc.getSessionMethod().getFullMethodName(), new Metadata());

            tracer.inboundMessageRead(0, 1, 1);
            tracer.inboundMessageRead(1, 1, 1);
            assertEquals(2, plane.inFlight());

            tracer.callbackStarted();
            assertEquals(2, plane.inFlight(), "starting the callback must not close its occupancy slot");
            plane.requestCompleted(0);
            assertEquals(1, plane.inFlight());

            tracer.streamClosed(io.grpc.Status.CANCELLED);
            assertEquals(0, plane.inFlight(), "stream close must release a callback that never started");
            assertThrows(IllegalStateException.class, tracer::callbackStarted);
        } finally {
            plane.close();
        }
    }
}
