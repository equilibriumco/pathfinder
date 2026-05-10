use p2p::preconfirmed::Event;
use tokio::sync::{mpsc, watch};

pub type PreconfirmedP2PEventProcessingTaskHandle = tokio::task::JoinHandle<anyhow::Result<()>>;

pub struct PreconfirmedTaskHandles {
    pub preconfirmed_p2p_event_processing_handle: PreconfirmedP2PEventProcessingTaskHandle,
    // Placeholder watched type, should be the preconfirmed block.
    pub preconfirmed_watch: Option<watch::Receiver<u32>>,
}

impl PreconfirmedTaskHandles {
    pub fn pending() -> Self {
        Self {
            preconfirmed_p2p_event_processing_handle: tokio::task::spawn(std::future::pending()),
            preconfirmed_watch: None,
        }
    }
}

pub fn start(p2p_event_rx: mpsc::UnboundedReceiver<Event>) -> PreconfirmedTaskHandles {
    let (watch_tx, watch_rx) = watch::channel(0);
    let jh = util::task::spawn(process_preconfirmed_p2p_events(p2p_event_rx, watch_tx));
    PreconfirmedTaskHandles {
        preconfirmed_p2p_event_processing_handle: jh,
        preconfirmed_watch: Some(watch_rx),
    }
}

async fn process_preconfirmed_p2p_events(
    mut p2p_event_rx: mpsc::UnboundedReceiver<Event>,
    preconfirmed_watch_tx: watch::Sender<u32>,
) -> anyhow::Result<()> {
    while let Some(event) = p2p_event_rx.recv().await {
        match event.kind {
            p2p::preconfirmed::EventKind::PreconfirmedTransactionsPlaceholder => {
                preconfirmed_watch_tx.send_modify(|x| *x += 1)
            }
        }
    }

    Ok(())
}
