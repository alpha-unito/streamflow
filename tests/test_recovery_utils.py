import pytest

from streamflow.recovery.utils import DirectedAcyclicGraph, DirectedGraph


@pytest.fixture
def g_full_connected() -> DirectedGraph:
    """Creates a complete directed graph on 3 nodes (0, 1, 2)."""
    g = DirectedGraph("K3")
    for u in [0, 1, 2]:
        for v in [0, 1, 2]:
            if u != v:
                g.add(u, v)
    return g


@pytest.fixture
def pipeline() -> DirectedGraph:
    """Creates a directed path 0 -> 1 -> 2."""
    g = DirectedGraph("P3")
    g.add(0, 1)
    g.add(1, 2)
    return g


@pytest.fixture
def g_loop() -> DirectedGraph:
    # 0 -> 1 -> 2 <-----------------------+
    #           |                         |
    #           v                         |
    #      +--- 3 ---+                    |
    #      |    |    |                    |
    #      v    v    v                    |
    #      4    5    10                   |
    #      |    |    |                    |
    #      v    14   11                   |
    #      12   |    |                    |
    #      |    +--> 6 --> 7 --> 8 -------+
    #      v         ^           |
    #      13 -------+           v
    #                            9 (Exit)
    g = DirectedGraph("Loop")
    for k, values in {
        0: [1],
        1: [2],
        2: [3],
        3: [4, 5, 10],
        10: [11],
        11: [6],
        4: [12],
        12: [13],
        13: [6],
        5: [14],
        14: [6],
        6: [7],
        7: [8],
        8: [2, 9],
        9: [],
    }.items():
        for v in values:
            g.add(k, v)
    return g


def test_successors(g_full_connected: DirectedGraph) -> None:
    assert g_full_connected.successors(0) == {1, 2}
    with pytest.raises(KeyError):
        g_full_connected.successors(-1)


def test_predecessors(g_full_connected: DirectedGraph) -> None:
    assert g_full_connected.predecessors(0) == {1, 2}
    with pytest.raises(KeyError):
        g_full_connected.predecessors(-1)


def test_in_degree(g_full_connected: DirectedGraph, pipeline: DirectedGraph) -> None:
    assert g_full_connected.in_degree() == {0: 2, 1: 2, 2: 2}
    assert pipeline.in_degree() == {0: 0, 1: 1, 2: 1}


def test_out_degree(g_full_connected: DirectedGraph, pipeline: DirectedGraph) -> None:
    assert g_full_connected.out_degree() == {0: 2, 1: 2, 2: 2}
    assert pipeline.out_degree() == {0: 1, 1: 1, 2: 0}


def test_add() -> None:
    g = DirectedGraph("test")
    g.add(0, 1)
    assert 1 in g._successors[0]
    assert 0 in g._predecessors[1]
    g.add(2, None)
    assert 2 in g.get_nodes()
    assert g.out_degree()[2] == 0


def test_remove_node(g_full_connected: DirectedGraph) -> None:
    # Test standard removal
    g_full_connected.remove_node(0, prune_dead_end=False)
    assert 0 not in g_full_connected.get_nodes()
    assert 0 not in g_full_connected.successors(1)
    assert 0 not in g_full_connected.predecessors(1)


def test_remove_node_prune() -> None:
    # 0 -> 1 -> 2
    g = DirectedGraph("prune")
    g.add(0, 1)
    g.add(1, 2)
    # Removing 2 should prune 1 then 0
    removed = g.remove_node(2, prune_dead_end=True)
    assert sorted(removed) == [0, 1, 2]
    assert len(g.get_nodes()) == 0


def test_remove_node_prune_2(g_loop: DirectedGraph) -> None:
    assert g_loop.successors(3) == {10, 4, 5}
    assert g_loop.predecessors(6) == {11, 13, 14}
    removed = g_loop.remove_node(13, prune_dead_end=True)
    assert set(removed) == {4, 12, 13}
    assert g_loop.successors(3) == {10, 5}
    assert g_loop.predecessors(6) == {11, 14}


def test_remove_node_prune_3(g_loop: DirectedGraph) -> None:
    removed = g_loop.remove_node(6, prune_dead_end=True)
    assert set(removed) == {0, 1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14}
    assert g_loop.get_nodes() == {7, 8, 9}
    assert g_loop.successors(7) == {8}
    assert g_loop.successors(8) == {9}
    assert len(g_loop.successors(9)) == 0


def test_replace(pipeline: DirectedGraph) -> None:
    pipeline.replace(1, "new_node")
    assert "new_node" in pipeline.get_nodes()
    assert 1 not in pipeline.get_nodes()
    assert pipeline.successors(0) == {"new_node"}
    assert pipeline.predecessors(2) == {"new_node"}
    with pytest.raises(ValueError):
        pipeline.replace(0, "new_node")  # new_node already exists


def test_sources_and_sinks() -> None:
    dag = DirectedAcyclicGraph("dag")
    dag.add(0, 1)
    dag.add(1, 2)
    assert dag.get_sources() == {0}
    assert dag.get_sinks() == {2}


def test_promote_to_source() -> None:
    # 0 -> 1, 2 -> 1, 1 -> 3
    dag = DirectedAcyclicGraph("promote")
    dag.add(0, 1)
    dag.add(2, 1)
    dag.add(1, 3)

    # Promoting 1 to source should detach 0 and 2.
    # Since 0 and 2 have no other successors, they should be pruned.
    removed = dag.promote_to_source(1)
    assert {0, 2} == set(removed)
    assert dag.get_sources() == {1}
    assert dag.successors(1) == {3}
