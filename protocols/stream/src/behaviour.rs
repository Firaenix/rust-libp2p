use core::fmt;
use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{channel::mpsc, StreamExt};
use libp2p_core::{multiaddr::Protocol, transport::PortUse, Endpoint, Multiaddr};
use libp2p_identity::PeerId;
use libp2p_swarm::{
    self as swarm, dial_opts::DialOpts, ConnectionDenied, ConnectionId, FromSwarm,
    NetworkBehaviour, THandler, THandlerInEvent, THandlerOutEvent, ToSwarm,
};
use swarm::{
    behaviour::ConnectionEstablished, dial_opts::PeerCondition, ConnectionClosed, DialError,
    DialFailure, ListenFailure,
};

use crate::{handler::Handler, shared::Shared, Control};

/// A generic behaviour for stream-oriented protocols.
pub struct Behaviour {
    shared: Arc<Mutex<Shared>>,
    dial_receiver: mpsc::Receiver<PeerId>,
    max_negotiating_outbound_streams: NonZeroUsize,
}

impl Default for Behaviour {
    fn default() -> Self {
        Self::new()
    }
}

impl Behaviour {
    pub fn new() -> Self {
        let (dial_sender, dial_receiver) = mpsc::channel(0);

        Self {
            shared: Arc::new(Mutex::new(Shared::new(dial_sender))),
            dial_receiver,
            max_negotiating_outbound_streams: NonZeroUsize::MIN,
        }
    }

    /// Sets how many outbound streams each connection negotiates at once; the default is 1.
    ///
    /// Above 1, an [`open_stream`](Control::open_stream) no longer waits a negotiation round trip
    /// behind every request queued ahead of it, but the remote receives streams in bursts, and a
    /// remote that accepts them slower than they arrive drops the excess.
    pub fn with_max_negotiating_outbound_streams(mut self, max: NonZeroUsize) -> Self {
        self.max_negotiating_outbound_streams = max;
        self
    }

    /// Obtain a new [`Control`].
    pub fn new_control(&self) -> Control {
        Control::new(self.shared.clone())
    }
}

fn is_relayed(addr: &Multiaddr) -> bool {
    addr.iter().any(|p| p == Protocol::P2pCircuit)
}

/// The protocol is already registered.
#[derive(Debug)]
pub struct AlreadyRegistered;

impl fmt::Display for AlreadyRegistered {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "The protocol is already registered")
    }
}

impl std::error::Error for AlreadyRegistered {}

impl NetworkBehaviour for Behaviour {
    type ConnectionHandler = Handler;
    type ToSwarm = ();

    fn handle_established_inbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        local_addr: &Multiaddr,
        _: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(Handler::new(
            peer,
            self.shared.clone(),
            Shared::lock(&self.shared).receiver(peer, connection_id, is_relayed(local_addr)),
            self.max_negotiating_outbound_streams,
        ))
    }

    fn handle_established_outbound_connection(
        &mut self,
        connection_id: ConnectionId,
        peer: PeerId,
        addr: &Multiaddr,
        _: Endpoint,
        _: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(Handler::new(
            peer,
            self.shared.clone(),
            Shared::lock(&self.shared).receiver(peer, connection_id, is_relayed(addr)),
            self.max_negotiating_outbound_streams,
        ))
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                ..
            }) => Shared::lock(&self.shared).on_connection_established(
                connection_id,
                peer_id,
                endpoint.is_relayed(),
            ),
            FromSwarm::ConnectionClosed(ConnectionClosed { connection_id, .. }) => {
                Shared::lock(&self.shared).on_connection_closed(connection_id)
            }
            FromSwarm::DialFailure(DialFailure {
                peer_id: Some(peer_id),
                error:
                    error @ (DialError::Transport(_)
                    | DialError::Denied { .. }
                    | DialError::NoAddresses
                    | DialError::Aborted
                    | DialError::WrongPeerId { .. }),
                connection_id,
            }) => {
                let reason = error.to_string(); // We can only forward the string repr but it is better than nothing.

                let mut shared = Shared::lock(&self.shared);
                shared.on_connection_denied(connection_id);
                shared.on_dial_failure(peer_id, reason)
            }
            FromSwarm::DialFailure(DialFailure {
                peer_id: Some(peer_id),
                error: DialError::DialPeerConditionFalse(_),
                ..
            }) => Shared::lock(&self.shared).on_dial_condition_false(peer_id),
            FromSwarm::ListenFailure(ListenFailure { connection_id, .. }) => {
                Shared::lock(&self.shared).on_connection_denied(connection_id)
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        _peer_id: PeerId,
        _connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        libp2p_core::util::unreachable(event);
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        if let Poll::Ready(Some(peer)) = self.dial_receiver.poll_next_unpin(cx) {
            return Poll::Ready(ToSwarm::Dial {
                opts: DialOpts::peer_id(peer)
                    .condition(PeerCondition::DisconnectedAndNotDialing)
                    .build(),
            });
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use futures::{channel::oneshot, task::noop_waker_ref};
    use libp2p_core::ConnectedPoint;
    use libp2p_swarm::{ConnectionHandler as _, ConnectionHandlerEvent, Stream, StreamProtocol};

    use super::*;
    use crate::{handler::NewStream, OpenStreamError};

    const STREAMS: usize = 16;

    fn behaviour() -> Behaviour {
        Behaviour::new().with_max_negotiating_outbound_streams(NonZeroUsize::new(STREAMS).unwrap())
    }

    fn direct_address() -> Multiaddr {
        Multiaddr::empty()
            .with(Protocol::Ip4(Ipv4Addr::LOCALHOST))
            .with(Protocol::Tcp(4001))
    }

    fn circuit_address() -> Multiaddr {
        direct_address()
            .with(Protocol::P2p(PeerId::random()))
            .with(Protocol::P2pCircuit)
    }

    fn establish(
        behaviour: &mut Behaviour,
        connection_id: ConnectionId,
        peer_id: PeerId,
        endpoint: &ConnectedPoint,
    ) {
        behaviour.on_swarm_event(FromSwarm::ConnectionEstablished(ConnectionEstablished {
            peer_id,
            connection_id,
            endpoint,
            failed_addresses: &[],
            other_established: 0,
        }));
    }

    fn dialled(behaviour: &mut Behaviour, id: usize, peer: PeerId, address: Multiaddr) -> Handler {
        let connection_id = ConnectionId::new_unchecked(id);
        let handler = behaviour
            .handle_established_outbound_connection(
                connection_id,
                peer,
                &address,
                Endpoint::Dialer,
                PortUse::Reuse,
            )
            .unwrap();
        let endpoint = ConnectedPoint::Dialer {
            address,
            role_override: Endpoint::Dialer,
            port_use: PortUse::Reuse,
        };
        establish(behaviour, connection_id, peer, &endpoint);
        handler
    }

    fn accepted(
        behaviour: &mut Behaviour,
        id: usize,
        peer: PeerId,
        local_addr: Multiaddr,
    ) -> Handler {
        let connection_id = ConnectionId::new_unchecked(id);
        let send_back_addr = direct_address();
        let handler = behaviour
            .handle_established_inbound_connection(
                connection_id,
                peer,
                &local_addr,
                &send_back_addr,
            )
            .unwrap();
        let endpoint = ConnectedPoint::Listener {
            local_addr,
            send_back_addr,
        };
        establish(behaviour, connection_id, peer, &endpoint);
        handler
    }

    fn request_streams(
        behaviour: &Behaviour,
        peer: PeerId,
    ) -> Vec<oneshot::Receiver<Result<Stream, OpenStreamError>>> {
        (0..STREAMS)
            .map(|_| {
                let (sender, reply) = oneshot::channel();
                Shared::lock(&behaviour.shared)
                    .sender(peer)
                    .try_send(NewStream {
                        protocol: StreamProtocol::new("/test"),
                        sender,
                    })
                    .unwrap();
                reply
            })
            .collect()
    }

    fn requested_negotiations(handler: &mut Handler) -> usize {
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut requested = 0;
        while let Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest { .. }) =
            handler.poll(&mut cx)
        {
            requested += 1;
        }
        requested
    }

    #[test]
    fn new_streams_use_a_direct_connection_over_a_dialled_circuit() {
        let mut behaviour = behaviour();
        let peer = PeerId::random();
        let mut relayed = dialled(&mut behaviour, 1, peer, circuit_address());
        let mut direct = accepted(&mut behaviour, 2, peer, direct_address());

        let _replies = request_streams(&behaviour, peer);

        assert_eq!(requested_negotiations(&mut direct), STREAMS);
        assert_eq!(requested_negotiations(&mut relayed), 0);
    }

    #[test]
    fn new_streams_use_a_direct_connection_over_an_accepted_circuit() {
        let mut behaviour = behaviour();
        let peer = PeerId::random();
        let mut relayed = accepted(&mut behaviour, 1, peer, circuit_address());
        let mut direct = dialled(&mut behaviour, 2, peer, direct_address());

        let _replies = request_streams(&behaviour, peer);

        assert_eq!(requested_negotiations(&mut direct), STREAMS);
        assert_eq!(requested_negotiations(&mut relayed), 0);
    }

    #[test]
    fn a_peer_reachable_only_over_a_relay_gets_its_streams_there() {
        let mut behaviour = behaviour();
        let peer = PeerId::random();
        let mut relayed = dialled(&mut behaviour, 1, peer, circuit_address());

        let _replies = request_streams(&behaviour, peer);

        assert_eq!(requested_negotiations(&mut relayed), STREAMS);
        assert!(
            behaviour.dial_receiver.try_next().is_err(),
            "no dial for a peer connected over a relay"
        );
    }

    #[test]
    fn new_streams_return_to_the_circuit_once_the_direct_connection_closes() {
        let mut behaviour = behaviour();
        let peer = PeerId::random();
        let mut relayed = dialled(&mut behaviour, 1, peer, circuit_address());
        let _direct = accepted(&mut behaviour, 2, peer, direct_address());

        Shared::lock(&behaviour.shared).on_connection_closed(ConnectionId::new_unchecked(2));
        let _replies = request_streams(&behaviour, peer);

        assert_eq!(requested_negotiations(&mut relayed), STREAMS);
    }
}
