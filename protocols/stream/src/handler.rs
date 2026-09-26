use std::{
    convert::Infallible,
    io,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{
    StreamExt as _,
    channel::{mpsc, oneshot},
};
use libp2p_identity::PeerId;
use libp2p_swarm::{
    self as swarm, ConnectionHandler, Stream, StreamProtocol,
    handler::{ConnectionEvent, DialUpgradeError, FullyNegotiatedInbound, FullyNegotiatedOutbound},
};

use crate::{OpenStreamError, shared::Shared, upgrade::Upgrade};

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
    use libp2p_core::{Endpoint, Multiaddr, transport::PortUse};
    use libp2p_swarm::{ConnectionId, NetworkBehaviour as _, StreamUpgradeError};

    use super::*;
    use crate::Behaviour;

    type Reply = oneshot::Receiver<Result<Stream, OpenStreamError>>;

    fn limited_to(max_negotiating_outbound_streams: usize) -> Behaviour {
        Behaviour::new().with_max_negotiating_outbound_streams(
            NonZeroUsize::new(max_negotiating_outbound_streams).unwrap(),
        )
    }

    fn connected_handler(mut behaviour: Behaviour) -> Handler {
        behaviour
            .handle_established_outbound_connection(
                ConnectionId::new_unchecked(1),
                PeerId::random(),
                &Multiaddr::empty(),
                Endpoint::Dialer,
                PortUse::Reuse,
            )
            .unwrap()
    }

    fn request_stream(handler: &Handler, protocol: &'static str) -> Reply {
        let (sender, reply) = oneshot::channel();
        Shared::lock(&handler.shared)
            .sender(handler.remote)
            .try_send(NewStream {
                protocol: StreamProtocol::new(protocol),
                sender,
            })
            .unwrap();
        reply
    }

    fn request_streams(handler: &Handler, count: usize) -> Vec<Reply> {
        (0..count)
            .map(|_| request_stream(handler, "/test"))
            .collect()
    }

    fn requested_negotiations(handler: &mut Handler) -> Vec<NewStream> {
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut requested = Vec::new();
        while let Poll::Ready(swarm::ConnectionHandlerEvent::OutboundSubstreamRequest {
            protocol,
        }) = handler.poll(&mut cx)
        {
            let (_, new_stream) = protocol.into_upgrade();
            requested.push(new_stream);
        }
        requested
    }

    fn fail_negotiation(handler: &mut Handler, new_stream: NewStream) {
        handler.on_connection_event(ConnectionEvent::DialUpgradeError(DialUpgradeError {
            info: new_stream,
            error: StreamUpgradeError::NegotiationFailed,
        }));
    }

    #[test]
    fn a_default_behaviour_negotiates_one_stream_at_a_time() {
        let mut handler = connected_handler(Behaviour::new());
        let _replies = request_streams(&handler, 3);

        let mut in_flight = requested_negotiations(&mut handler);
        assert_eq!(in_flight.len(), 1);

        fail_negotiation(&mut handler, in_flight.remove(0));
        assert_eq!(requested_negotiations(&mut handler).len(), 1);
    }

    #[test]
    fn an_opted_in_behaviour_negotiates_as_many_streams_at_once_as_it_allows() {
        let mut handler = connected_handler(limited_to(16));
        let _replies = request_streams(&handler, 16);

        assert_eq!(requested_negotiations(&mut handler).len(), 16);
    }

    #[test]
    fn requests_beyond_the_limit_wait_for_a_negotiation_to_settle() {
        let mut handler = connected_handler(limited_to(4));
        let _replies = request_streams(&handler, 10);

        let mut in_flight = requested_negotiations(&mut handler);
        assert_eq!(in_flight.len(), 4);
        assert!(requested_negotiations(&mut handler).is_empty());

        fail_negotiation(&mut handler, in_flight.remove(0));
        assert_eq!(requested_negotiations(&mut handler).len(), 1);
    }

    #[test]
    fn a_failed_negotiation_answers_only_its_own_caller() {
        let mut handler = connected_handler(limited_to(2));
        let mut reply_a = request_stream(&handler, "/a");
        let mut reply_b = request_stream(&handler, "/b");
        let mut in_flight = requested_negotiations(&mut handler);
        let b = in_flight
            .iter()
            .position(|new_stream| new_stream.protocol.as_ref() == "/b")
            .unwrap();

        fail_negotiation(&mut handler, in_flight.remove(b));

        assert!(matches!(
            reply_b.try_recv(),
            Ok(Some(Err(OpenStreamError::UnsupportedProtocol(p)))) if p.as_ref() == "/b"
        ));
        assert!(matches!(reply_a.try_recv(), Ok(None)));
    }

    #[test]
    fn a_request_whose_caller_stopped_waiting_is_not_negotiated() {
        let mut handler = connected_handler(Behaviour::new());
        drop(request_stream(&handler, "/a"));
        drop(request_stream(&handler, "/b"));
        let _reply_c = request_stream(&handler, "/c");

        let negotiated: Vec<_> = requested_negotiations(&mut handler)
            .into_iter()
            .map(|new_stream| new_stream.protocol)
            .collect();

        assert_eq!(negotiated, [StreamProtocol::new("/c")]);
    }
}
