import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class AgentNode:
    id: str
    name: str
    description: str
    subjects: List[str]
    route_name: str
    match_keywords: List[str]
    priority: int = 0

    def matches(self, message: str) -> bool:
        normalized = message.lower().strip()
        return any(keyword in normalized for keyword in self.match_keywords)


@dataclass
class OrchestrationGraph:
    nodes: Dict[str, AgentNode]
    edges: Dict[str, List[str]] = field(default_factory=dict)

    def get_node(self, node_id: str) -> Optional[AgentNode]:
        return self.nodes.get(node_id)

    def find_best_node(self, message: str) -> Optional[AgentNode]:
        candidates = [node for node in self.nodes.values() if node.matches(message)]
        if not candidates:
            return None
        return sorted(candidates, key=lambda node: node.priority, reverse=True)[0]


SHIFT_AGENT = AgentNode(
    id='shift-agent',
    name='Shift Agent',
    description='Handles shift scheduling, roster, and time-off questions.',
    subjects=['chat.incoming'],
    route_name='chat.incoming',
    match_keywords=[
        'shift',
        'schedule',
        'roster',
        'work time',
        'shift change',
        'shift swap',
        'shift start',
        'shift end',
        'time off',
    ],
    priority=10,
)

KPI_AGENT = AgentNode(
    id='kpi-agent',
    name='KPI Agent',
    description='Handles KPI, metrics, dashboards and performance questions.',
    subjects=['chat.kpi'],
    route_name='chat.kpi',
    match_keywords=[
        'kpi',
        'metric',
        'performance',
        'goal',
        'target',
        'indicator',
        'dashboard',
        'trend',
    ],
    priority=20,
)


def build_local_graph() -> OrchestrationGraph:
    graph = OrchestrationGraph(
        nodes={
            SHIFT_AGENT.id: SHIFT_AGENT,
            KPI_AGENT.id: KPI_AGENT,
        },
        edges={
            'orchestrator': [KPI_AGENT.id, SHIFT_AGENT.id],
        },
    )
    return graph


graph = build_local_graph()
