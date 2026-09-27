use std::{
    collections::{hash_map::Entry, HashMap},
    io,
    sync::{Arc, Mutex, MutexGuard},
};

use futures::channel::mpsc;
use libp2p_identity::PeerId;
use libp2p_swarm::{ConnectionId, Stream, StreamProtocol};
use rand::seq::IteratorRandom as _;

use crate::{handler::NewStream, AlreadyRegistered, IncomingStreams};

pub(crate) struct Shared {
    /// Tracks the supported inbound protocols created via
    /// [`Control::accept`](crate::Control::accept).
    ///
    /// For each [`StreamProtocol`], we hold the [`mpsc::Sender`] corresponding to the
    /// [`mpsc::Receiver`] in [`IncomingStreams`].
    supported_inbound_protocols: HashMap<StreamProtocol, mpsc::Sender<(PeerId, Stream)>>,

    connections: HashMap<ConnectionId, PeerId>,
    senders: HashMap<ConnectionId, mpsc::Sender<NewStream>>,

    /// Tracks channel pairs for a peer whilst we are dialing them.
    pending_channels: HashMap<PeerId, (mpsc::Sender<NewStream>, mpsc::Receiver<NewStream>)>,

    /// Sender for peers we want to dial.
    ///
    /// We manage this through a channel to avoid locks as part of
    /// [`NetworkBehaviour::poll`](libp2p_swarm::NetworkBehaviour::poll).
    dial_sender: mpsc::Sender<PeerId>,
}

impl Shared {
    pub(crate) fn lock(shared: &Arc<Mutex<Shared>>) -> MutexGuard<'_, Shared> {
        shared.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Shared {
    pub(crate) fn new(dial_sender: mpsc::Sender<PeerId>) -> Self {
        Self {
            dial_sender,
            connections: Default::default(),
            senders: Default::default(),
            pending_channels: Default::default(),
            supported_inbound_protocols: Default::default(),
        }
    }

    pub(crate) fn accept(
        &mut self,
        protocol: StreamProtocol,
    ) -> Result<IncomingStreams, AlreadyRegistered> {
        self.supported_inbound_protocols
            .retain(|_, sender| !sender.is_closed());

        if self.supported_inbound_protocols.contains_key(&protocol) {
            return Err(AlreadyRegistered);
        }

        let (sender, receiver) = mpsc::channel(0);
        self.supported_inbound_protocols
            .insert(protocol.clone(), sender);

        Ok(IncomingStreams::new(receiver))
    }

    /// Lists the protocols for which we have an active [`IncomingStreams`] instance.
    pub(crate) fn supported_inbound_protocols(&mut self) -> Vec<StreamProtocol> {
        self.supported_inbound_protocols
            .retain(|_, sender| !sender.is_closed());

        self.supported_inbound_protocols.keys().cloned().collect()
    }

    pub(crate) fn on_inbound_stream(
        &mut self,
        remote: PeerId,
        stream: Stream,
        protocol: StreamProtocol,
    ) {
        match self.supported_inbound_protocols.entry(protocol.clone()) {
            Entry::Occupied(mut entry) => match entry.get_mut().try_send((remote, stream)) {
                Ok(()) => {}
                Err(e) if e.is_full() => {
                    tracing::debug!(%protocol, "Channel is full, dropping inbound stream");
                }
                Err(e) if e.is_disconnected() => {
                    tracing::debug!(%protocol, "Channel is gone, dropping inbound stream");
                    entry.remove();
                }
                _ => unreachable!(),
            },
            Entry::Vacant(_) => {
                tracing::debug!(%protocol, "channel is gone, dropping inbound stream");
            }
        }
    }

    pub(crate) fn on_connection_established(&mut self, conn: ConnectionId, peer: PeerId) {
        self.connections.insert(conn, peer);
    }

    pub(crate) fn on_connection_closed(&mut self, conn: ConnectionId) {
        self.connections.remove(&conn);
        self.senders.remove(&conn);
    }

    /// Another behaviour denied a connection after our handler for it was created.
    pub(crate) fn on_connection_denied(&mut self, conn: ConnectionId) {
        self.on_connection_closed(conn);
    }

    /// The swarm skipped our dial because its condition was false.
    ///
    /// If the peer is connected, requests parked for the dial would never be delivered, so they
    /// fail instead of hanging. If a dial is merely in flight, the resulting connection picks
    /// them up.
    pub(crate) fn on_dial_condition_false(&mut self, peer: PeerId) {
        if !self.connections.values().any(|p| *p == peer) {
            return;
        }

        self.on_dial_failure(
            peer,
            "peer connected while a dial was pending; open the stream again".to_owned(),
        );
    }

    pub(crate) fn on_dial_failure(&mut self, peer: PeerId, reason: String) {
        let Some((_, mut receiver)) = self.pending_channels.remove(&peer) else {
            return;
        };

        while let Ok(Some(new_stream)) = receiver.try_next() {
            let _ = new_stream
                .sender
                .send(Err(crate::OpenStreamError::Io(io::Error::new(
                    io::ErrorKind::NotConnected,
                    reason.clone(),
                ))));
        }
    }

    pub(crate) fn sender(&mut self, peer: PeerId) -> mpsc::Sender<NewStream> {
        let maybe_sender = self
            .connections
            .iter()
            .filter_map(|(c, p)| (p == &peer).then_some(c))
            .choose(&mut rand::thread_rng())
            .and_then(|c| self.senders.get(c));

        match maybe_sender {
            Some(sender) => {
                tracing::debug!("Returning sender to existing connection");

                sender.clone()
            }
            None => {
                tracing::debug!(%peer, "Not connected to peer, initiating dial");

                let (sender, _) = self
                    .pending_channels
                    .entry(peer)
                    .or_insert_with(|| mpsc::channel(0));

                let _ = self.dial_sender.try_send(peer);

                sender.clone()
            }
        }
    }

    pub(crate) fn receiver(
        &mut self,
        peer: PeerId,
        connection: ConnectionId,
    ) -> mpsc::Receiver<NewStream> {
        // Registered with the handler rather than on `ConnectionEstablished`, so a stream
        // requested in between reaches this connection instead of dialling a connected peer.
        self.connections.insert(connection, peer);

        if let Some((sender, receiver)) = self.pending_channels.remove(&peer) {
            tracing::debug!(%peer, %connection, "Returning existing pending receiver");

            self.senders.insert(connection, sender);
            return receiver;
        }

        tracing::debug!(%peer, %connection, "Creating new channel pair");

        let (sender, receiver) = mpsc::channel(0);
        self.senders.insert(connection, sender);

        receiver
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared() -> Shared {
        let (dial_sender, _dial_receiver) = mpsc::channel(0);
        Shared::new(dial_sender)
    }

    #[test]
    fn connection_close_prunes_senders() {
        let mut shared = shared();
        let peer = PeerId::random();
        let conn = ConnectionId::new_unchecked(1);

        shared.on_connection_established(conn, peer);
        let _receiver = shared.receiver(peer, conn);
        assert_eq!(shared.senders.len(), 1);

        shared.on_connection_closed(conn);
        assert!(shared.senders.is_empty());
        assert!(shared.connections.is_empty());
    }

    #[test]
    fn a_stream_requested_before_connection_established_uses_the_new_connection() {
        let (dial_sender, mut dial_receiver) = mpsc::channel(1);
        let mut shared = Shared::new(dial_sender);
        let peer = PeerId::random();
        let conn = ConnectionId::new_unchecked(1);

        let _receiver = shared.receiver(peer, conn);
        let _sender = shared.sender(peer);

        assert!(shared.pending_channels.is_empty());
        assert!(
            dial_receiver.try_next().is_err(),
            "no dial for a connected peer"
        );
    }

    #[test]
    fn a_stream_requested_before_a_dialled_connection_is_established_uses_it() {
        let (dial_sender, mut dial_receiver) = mpsc::channel(1);
        let mut shared = Shared::new(dial_sender);
        let peer = PeerId::random();
        let conn = ConnectionId::new_unchecked(1);

        let _parked = shared.sender(peer);
        assert_eq!(dial_receiver.try_next().unwrap(), Some(peer));
        let _receiver = shared.receiver(peer, conn);
        let _sender = shared.sender(peer);

        assert!(shared.pending_channels.is_empty());
        assert!(
            dial_receiver.try_next().is_err(),
            "no second dial for a peer whose dial just connected"
        );
    }

    #[test]
    fn a_skipped_dial_to_a_connected_peer_fails_parked_requests() {
        let mut shared = shared();
        let peer = PeerId::random();
        let mut parked = shared.sender(peer);
        let (sender, mut reply) = futures::channel::oneshot::channel();
        parked
            .try_send(NewStream {
                protocol: StreamProtocol::new("/test"),
                sender,
            })
            .unwrap();

        shared.on_connection_established(ConnectionId::new_unchecked(3), peer);
        shared.on_dial_condition_false(peer);

        assert!(matches!(
            reply.try_recv(),
            Ok(Some(Err(crate::OpenStreamError::Io(_))))
        ));
        assert!(shared.pending_channels.is_empty());
    }

    #[test]
    fn a_skipped_dial_while_another_dial_is_in_flight_keeps_parked_requests() {
        let mut shared = shared();
        let peer = PeerId::random();
        let _parked = shared.sender(peer);

        shared.on_dial_condition_false(peer);

        assert!(shared.pending_channels.contains_key(&peer));
    }

    #[test]
    fn a_denied_connection_leaves_nothing_behind() {
        let mut shared = shared();
        let peer = PeerId::random();
        let conn = ConnectionId::new_unchecked(1);

        let _receiver = shared.receiver(peer, conn);
        shared.on_connection_denied(conn);

        assert!(shared.senders.is_empty());
        assert!(shared.connections.is_empty());
    }

    #[test]
    fn reconnect_churn_does_not_accumulate_senders() {
        let mut shared = shared();
        let peer = PeerId::random();

        for i in 0..100 {
            let conn = ConnectionId::new_unchecked(i);
            shared.on_connection_established(conn, peer);
            let _receiver = shared.receiver(peer, conn);
            shared.on_connection_closed(conn);
        }

        assert!(shared.senders.is_empty());
    }
}
