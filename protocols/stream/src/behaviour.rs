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
