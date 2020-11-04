from kubernetes.client.models.v1_node_selector_requirement import V1NodeSelectorRequirement
from kubernetes.client.models.v1_node_selector_term import V1NodeSelectorTerm

from clusterman.kubernetes.util import selector_term_matches_requirement


def test_selector_term_matches_requirement():
    selector_term = [V1NodeSelectorTerm(
        match_expressions=[
            V1NodeSelectorRequirement(
                key='clusterman.com/scheduler',
                operator='Exists'
            ),
            V1NodeSelectorRequirement(
                key='clusterman.com/pool',
                operator='In',
                values=['bar']
            )
        ]
    )]
    selector_requirement = V1NodeSelectorRequirement(
        key='clusterman.com/pool',
        operator='In',
        values=['bar']
    )
    assert selector_term_matches_requirement(selector_term, selector_requirement)
