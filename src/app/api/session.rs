use crate::api::schema::{RemoteAgentInfo, ResponseResult, SessionSnapshot};
use crate::app::App;
use crate::remote_agents::RemoteHostKey;

use super::responses::encode_success;

impl App {
    pub(super) fn handle_session_snapshot(&mut self, id: String) -> String {
        encode_success(
            id,
            ResponseResult::SessionSnapshot {
                snapshot: Box::new(self.session_snapshot()),
            },
        )
    }

    fn session_snapshot(&self) -> SessionSnapshot {
        let focused_workspace_id = self
            .state
            .active
            .map(|ws_idx| self.public_workspace_id(ws_idx));
        let focused_tab_id = self.state.active.and_then(|ws_idx| {
            let ws = self.state.workspaces.get(ws_idx)?;
            self.public_tab_id(ws_idx, ws.active_tab)
        });
        let focused_pane_id = self.state.active.and_then(|ws_idx| {
            let ws = self.state.workspaces.get(ws_idx)?;
            self.public_pane_id(ws_idx, ws.focused_pane_id()?)
        });

        let mut workspaces = Vec::new();
        let mut tabs = Vec::new();
        let mut layouts = Vec::new();
        for (ws_idx, ws) in self.state.workspaces.iter().enumerate() {
            workspaces.push(self.workspace_info(ws_idx));
            for tab_idx in 0..ws.tabs.len() {
                if let Some(tab) = self.tab_info(ws_idx, tab_idx) {
                    tabs.push(tab);
                }
                if let Some(layout) = self.pane_layout_snapshot(ws_idx, tab_idx) {
                    layouts.push(layout);
                }
            }
        }

        SessionSnapshot {
            version: crate::build_info::version(),
            protocol: crate::protocol::PROTOCOL_VERSION,
            focused_workspace_id,
            focused_tab_id,
            focused_pane_id,
            workspaces,
            tabs,
            panes: self.collect_panes_for_workspace(None).unwrap_or_default(),
            layouts,
            agents: self.collect_agent_infos(),
            remote_agents: self.remote_agent_infos(),
        }
    }

    fn remote_agent_infos(&self) -> Vec<RemoteAgentInfo> {
        self.state
            .remote_agents
            .panel_agents()
            .into_iter()
            .map(|agent| {
                let (source_target, source_session) = RemoteHostKey::split_key(&agent.host_key);
                let presentation = agent.presentation;
                RemoteAgentInfo {
                    source_target: source_target.to_string(),
                    source_session: source_session.to_string(),
                    source_label: agent.host_label,
                    source_online: agent.online,
                    workspace_id: presentation.workspace_id,
                    tab_id: presentation.tab_id,
                    pane_id: presentation.pane_id,
                    workspace_label: presentation.workspace_label,
                    tab_label: presentation.tab_label,
                    title: presentation.pane_label,
                    terminal_title: presentation.terminal_title,
                    terminal_title_stripped: presentation.terminal_title_stripped,
                    agent: presentation.agent_kind_label,
                    display_agent: presentation.agent_label,
                    agent_status: crate::remote_agents::agent_status_for_presentation(
                        presentation.state,
                        presentation.seen,
                    ),
                    state_labels: presentation.state_labels,
                    tokens: presentation.tokens,
                    state_change_seq: agent.state_change_seq,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::api::schema::{EmptyParams, Method, ResponseResult, SuccessResponse};
    use crate::{config::Config, workspace::Workspace};

    fn app_with_two_tabs() -> crate::app::App {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = crate::app::App::new(
            &Config::default(),
            true,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        let mut workspace = Workspace::test_new("snapshot");
        workspace.test_add_tab(None);
        app.state.workspaces = vec![workspace];
        app.state.ensure_test_terminals();
        app.state.active = Some(0);
        app
    }

    #[test]
    fn session_snapshot_reports_remote_agents() {
        use crate::api::schema::AgentStatus;
        use crate::remote_agents::{
            RemoteAgentPresentation, RemoteAgentSnapshot, RemoteAgentUpdate, RemoteAgentUpdateKind,
            RemoteHostKey, RemoteHostRegistration,
        };

        let mut app = app_with_two_tabs();
        let source = crate::config::RemoteAgentSourceConfig {
            target: "user@box".into(),
            label: Some("box".into()),
            session: "work".into(),
        };
        let key = RemoteHostKey::for_source(&source);
        app.state.remote_agents.reconcile(&[RemoteHostRegistration {
            key: key.clone(),
            label: source.fleet_label(),
            generation: 1,
            order: 0,
        }]);
        assert!(app.state.remote_agents.apply_update(
            RemoteAgentUpdate::immediate(
                key,
                1,
                RemoteAgentUpdateKind::Snapshot(RemoteAgentSnapshot {
                    version: "0.8.0".into(),
                    protocol: crate::protocol::PROTOCOL_VERSION,
                    agents: vec![RemoteAgentPresentation {
                        workspace_id: "workspace:1".into(),
                        tab_id: "tab:1".into(),
                        pane_id: "pane:1".into(),
                        workspace_label: "repo".into(),
                        tab_label: None,
                        pane_label: None,
                        terminal_title: None,
                        terminal_title_stripped: None,
                        agent_label: "claude".into(),
                        agent_kind_label: Some("claude".into()),
                        agent: Some(crate::detect::Agent::Claude),
                        state: crate::detect::AgentState::Idle,
                        seen: false,
                        state_labels: std::collections::HashMap::new(),
                        tokens: std::collections::HashMap::new(),
                        display_order: (0, 0, 0),
                        order: (0, 0, 0),
                    }],
                }),
            ),
            &mut app.state.next_agent_state_change_seq,
        ));

        let response = app.handle_api_request(crate::api::schema::Request {
            id: "req_snapshot".into(),
            method: Method::SessionSnapshot(EmptyParams::default()),
        });

        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        let ResponseResult::SessionSnapshot { snapshot } = success.result else {
            panic!("expected session snapshot response");
        };
        assert_eq!(snapshot.remote_agents.len(), 1);
        let remote = &snapshot.remote_agents[0];
        assert_eq!(remote.source_target, "user@box");
        assert_eq!(remote.source_session, "work");
        assert_eq!(remote.source_label, "box/work");
        assert!(remote.source_online);
        assert_eq!(remote.pane_id, "pane:1");
        assert_eq!(remote.display_agent, "claude");
        assert_eq!(remote.agent_status, AgentStatus::Done);
    }

    #[test]
    fn session_snapshot_bootstraps_runtime_resources() {
        let mut app = app_with_two_tabs();
        let response = app.handle_api_request(crate::api::schema::Request {
            id: "req_snapshot".into(),
            method: Method::SessionSnapshot(EmptyParams::default()),
        });

        let success: SuccessResponse = serde_json::from_str(&response).unwrap();
        let ResponseResult::SessionSnapshot { snapshot } = success.result else {
            panic!("expected session snapshot response");
        };
        assert_eq!(success.id, "req_snapshot");
        assert_eq!(snapshot.workspaces.len(), 1);
        assert_eq!(snapshot.tabs.len(), 2);
        assert_eq!(snapshot.panes.len(), 2);
        assert_eq!(snapshot.layouts.len(), 2);
        assert_eq!(
            snapshot.focused_workspace_id.as_deref(),
            Some(snapshot.workspaces[0].workspace_id.as_str())
        );
        assert_eq!(
            snapshot.focused_tab_id.as_deref(),
            Some(snapshot.tabs[0].tab_id.as_str())
        );
        assert_eq!(
            snapshot.focused_pane_id.as_deref(),
            Some(snapshot.panes[0].pane_id.as_str())
        );
    }
}
