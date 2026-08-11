use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::agents::AgentInfo;
use super::common::AgentStatus;
use super::panes::{PaneInfo, PaneLayoutSnapshot};
use super::tabs::TabInfo;
use super::workspaces::WorkspaceInfo;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionSnapshot {
    pub version: String,
    pub protocol: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focused_workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focused_tab_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub focused_pane_id: Option<String>,
    pub workspaces: Vec<WorkspaceInfo>,
    pub tabs: Vec<TabInfo>,
    pub panes: Vec<PaneInfo>,
    pub layouts: Vec<PaneLayoutSnapshot>,
    pub agents: Vec<AgentInfo>,
    /// Agents observed on configured remote agent sources. Read-only in this
    /// protocol: their ids belong to the remote session and cannot be used as
    /// targets for local commands.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remote_agents: Vec<RemoteAgentInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RemoteAgentInfo {
    /// Connection target of the remote agent source this agent came from.
    pub source_target: String,
    /// Remote session name of the source.
    pub source_session: String,
    pub source_label: String,
    /// Whether the source's snapshot stream is currently connected.
    pub source_online: bool,
    /// Workspace id within the remote session.
    pub workspace_id: String,
    /// Tab id within the remote session.
    pub tab_id: String,
    /// Pane id within the remote session.
    pub pane_id: String,
    pub workspace_label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tab_label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_title_stripped: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    pub display_agent: String,
    pub agent_status: AgentStatus,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub state_labels: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    #[schemars(schema_with = "super::common::metadata_token_values_schema")]
    pub tokens: HashMap<String, String>,
    /// Local observation sequence bumped when the agent's status changes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_change_seq: Option<u64>,
}
