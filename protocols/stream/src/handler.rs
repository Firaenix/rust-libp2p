use std::{
    convert::Infallible,
    io,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{
    channel::{mpsc, oneshot},
    StreamExt as _,
};
use libp2p_identity::PeerId;
use libp2p_swarm::{
    self as swarm,
    handler::{ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound},
    ConnectionHandler, Stream, StreamProtocol,
};

use crate::{shared::Shared, upgrade::Upgrade, OpenStreamError};

pub struct Handler {
    remote: PeerId,
    shared: Arc<Mutex<Shared>>,

    receiver: mpsc::Receiver<NewStream>,
    negotiating_outbound_streams: usize,
    max_negotiating_outbound_streams: NonZeroUsize,
}

impl Handler {
    pub(crate) fn new(
        remote: PeerId,
        shared: Arc<Mutex<Shared>>,
        receiver: mpsc::Receiver<NewStream>,
        max_negotiating_outbound_streams: NonZeroUsize,
    ) -> Self {
        Self {
            shared,
            receiver,
            negotiating_outbound_streams: 0,
            max_negotiating_outbound_streams,
            remote,
        }
    }
}

impl ConnectionHandler for Handler {
    type FromBehaviour = Infallible;
    type ToBehaviour = Infallible;
    type InboundProtocol = Upgrade;
    type OutboundProtocol = Upgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = NewStream;

    fn listen_protocol(&self) -> swarm::SubstreamProtocol<Self::InboundProtocol> {
        swarm::SubstreamProtocol::new(
            Upgrade {
                supported_protocols: Shared::lock(&self.shared).supported_inbound_protocols(),
            },
            (),
        )
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<swarm::ConnectionHandlerEvent<Self::OutboundProtocol, NewStream, Self::ToBehaviour>>
    {
        // The connection polls its handler again after every negotiation result, so reaching the
        // limit needs no waker.
        while self.negotiating_outbound_streams < self.max_negotiating_outbound_streams.get() {
            let Poll::Ready(Some(new_stream)) = self.receiver.poll_next_unpin(cx) else {
                return Poll::Pending;
            };
            if new_stream.sender.is_canceled() {
                tracing::debug!(
                    protocol = %new_stream.protocol,
                    "Caller stopped waiting, not negotiating stream"
                );
                continue;
            }

            self.negotiating_outbound_streams += 1;
            let supported_protocols = vec![new_stream.protocol.clone()];
            return Poll::Ready(swarm::ConnectionHandlerEvent::OutboundSubstreamRequest {
                protocol: swarm::SubstreamProtocol::new(
                    Upgrade {
                        supported_protocols,
                    },
                    new_stream,
                ),
            });
        }

        Poll::Pending
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        libp2p_core::util::unreachable(event)
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol: (stream, protocol),
                info: (),
            }) => {
                Shared::lock(&self.shared).on_inbound_stream(self.remote, stream, protocol);
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol: (stream, actual_protocol),
                info: new_stream,
            }) => {
                self.negotiating_outbound_streams =
                    self.negotiating_outbound_streams.saturating_sub(1);
                debug_assert_eq!(new_stream.protocol, actual_protocol);

                let _ = new_stream.sender.send(Ok(stream));
            }
            ConnectionEvent::DialUpgradeError(DialUpgradeError {
                error,
                info: new_stream,
            }) => {
                self.negotiating_outbound_streams =
                    self.negotiating_outbound_streams.saturating_sub(1);
                let NewStream {
                    protocol: p,
                    sender,
                } = new_stream;

                let error = match error {
                    swarm::StreamUpgradeError::Timeout => {
                        OpenStreamError::Io(io::Error::from(io::ErrorKind::TimedOut))
                    }
                    swarm::StreamUpgradeError::Apply(v) => libp2p_core::util::unreachable(v),
                    swarm::StreamUpgradeError::NegotiationFailed => {
                        OpenStreamError::UnsupportedProtocol(p)
                    }
                    swarm::StreamUpgradeError::Io(io) => OpenStreamError::Io(io),
                };

                let _ = sender.send(Err(error));
            }
            _ => {}
        }
    }
}

/// Message from a [`Control`](crate::Control) to
/// a [`ConnectionHandler`] to negotiate a new outbound stream.
///
/// It is also the negotiation's open info, so the result finds its requester however many
/// negotiations are in flight.
#[derive(Debug)]
pub struct NewStream {
    pub(crate) protocol: StreamProtocol,
    pub(crate) sender: oneshot::Sender<Result<Stream, OpenStreamError>>,
}

#[cfg(test)]
mod tests {
    use futures::task::noop_waker_ref;

    use super::*;

    const MAX_NEGOTIATING_OUTBOUND_STREAMS: usize = 128;

    type Reply = oneshot::Receiver<Result<Stream, OpenStreamError>>;

    fn handler_with_requests(count: usize) -> (Handler, Vec<Reply>) {
        let (dial_sender, _dial_receiver) = mpsc::channel(0);
        let shared = Arc::new(Mutex::new(Shared::new(dial_sender)));
        let (mut requests, receiver) = mpsc::channel(count);
        let mut replies = Vec::new();
        for _ in 0..count {
            let (sender, reply) = oneshot::channel();
            requests
                .try_send(NewStream {
                    protocol: StreamProtocol::new("/test"),
                    sender,
                })
                .unwrap();
            replies.push(reply);
        }
        let handler = Handler::new(
            PeerId::random(),
            shared,
            receiver,
            NonZeroUsize::new(MAX_NEGOTIATING_OUTBOUND_STREAMS).unwrap(),
        );
        (handler, replies)
    }

    fn requested_negotiations(handler: &mut Handler) -> usize {
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut requested = 0;
        while let Poll::Ready(swarm::ConnectionHandlerEvent::OutboundSubstreamRequest { .. }) =
            handler.poll(&mut cx)
        {
            requested += 1;
        }
        requested
    }

    #[test]
    fn queued_requests_negotiate_at_the_same_time() {
        let (mut handler, _replies) = handler_with_requests(8);

        assert_eq!(requested_negotiations(&mut handler), 8);
    }

    #[test]
    fn concurrent_negotiations_are_capped() {
        let (mut handler, _replies) = handler_with_requests(MAX_NEGOTIATING_OUTBOUND_STREAMS + 5);

        assert_eq!(
            requested_negotiations(&mut handler),
            MAX_NEGOTIATING_OUTBOUND_STREAMS
        );
    }
}
