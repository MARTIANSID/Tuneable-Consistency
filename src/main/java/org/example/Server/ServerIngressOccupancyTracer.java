package org.example.Server;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;

/**
 * Opens a server occupancy slot when gRPC finishes deframing a request,
 * before the request waits to enter {@code KvClientService.onNext}.
 */
final class ServerIngressOccupancyTracer extends ServerStreamTracer {

    private static final Context.Key<ServerIngressOccupancyTracer> CONTEXT_KEY =
            Context.key("kv-ingress-occupancy-tracer");
    private static final ServerStreamTracer NOOP = new ServerStreamTracer() {
    };

    private final MeasurementPlane plane;
    private int callbacksPending;

    private ServerIngressOccupancyTracer(MeasurementPlane plane) {
        this.plane = plane;
    }

    static Factory factory(MeasurementPlane plane) {
        String sessionMethod = org.example.raft.KvClientGrpc.getSessionMethod().getFullMethodName();
        return new Factory() {
            @Override
            public ServerStreamTracer newServerStreamTracer(String fullMethodName, Metadata headers) {
                return sessionMethod.equals(fullMethodName) ? new ServerIngressOccupancyTracer(plane) : NOOP;
            }
        };
    }

    static ServerIngressOccupancyTracer current() {
        return CONTEXT_KEY.get();
    }

    @Override
    public Context filterContext(Context context) {
        return context.withValue(CONTEXT_KEY, this);
    }

    @Override
    public synchronized void inboundMessageRead(int seqNo, long optionalWireSize, long optionalUncompressedSize) {
        callbacksPending++;
        plane.requestAdmitted();
    }

    synchronized void callbackStarted() {
        if (callbacksPending == 0) {
            throw new IllegalStateException("KvClientService.onNext has no matching deframed request");
        }
        callbacksPending--;
    }

    @Override
    public void streamClosed(Status status) {
        int abandoned;
        synchronized (this) {
            abandoned = callbacksPending;
            callbacksPending = 0;
        }
        if (abandoned > 0) {
            plane.requestsAbandonedBeforeCallback(abandoned);
        }
    }
}
