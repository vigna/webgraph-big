/*
 * Copyright (C) 2021-2022 Antoine Pietri
 *
 * This program and the accompanying materials are made available under the
 * terms of the GNU Lesser General Public License v2.1 or later,
 * which is available at
 * http://www.gnu.org/licenses/old-licenses/lgpl-2.1-standalone.html,
 * or the Apache Software License 2.0, which is available at
 * https://www.apache.org/licenses/LICENSE-2.0.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later OR Apache-2.0
 */

package it.unimi.dsi.big.webgraph;

import it.unimi.dsi.fastutil.longs.LongIterator;

/**
 * A wrapper class exhibiting a {@linkplain #graph} and its {@linkplain #transpose} as a
 * bidirectional graph. Methods such as {@link #predecessors(long)}, {@link #indegrees()}, etc. are
 * implemented using the transpose.
 */
public class BidirectionalImmutableGraph extends ImmutableGraph {
	/** A graph. */
	public final ImmutableGraph graph;
	/** The transpose of {@link #graph}. */
	public final ImmutableGraph transpose;

    /**
	 * Creates a bidirectional immutable graph.
	 *
	 * @param graph a graph.
	 * @param transpose its transpose.
	 */
	public BidirectionalImmutableGraph(final ImmutableGraph graph, final ImmutableGraph transpose) {
		this.graph = graph;
		this.transpose = transpose;
		if (graph.numNodes() != transpose.numNodes()) throw new IllegalArgumentException("The graph and its transpose graph have a different number of nodes");
		if (graph.numArcs() != transpose.numArcs()) throw new IllegalArgumentException("The graph and its transpose graph have a different number of arcs");
    }

    @Override
    public long numNodes() {
		return this.graph.numNodes();
    }

    @Override
    public long numArcs() {
		return this.graph.numArcs();
    }

	/**
	 * {@inheritDoc}
	 *
	 * @implSpec This methods returns true if both {@link #graph} and {@link #transpose} provide random
	 *           access.
	 */
    @Override
    public boolean randomAccess() {
		return this.graph.randomAccess() && this.transpose.randomAccess();
    }

	/**
	 * {@inheritDoc}
	 *
	 * @implSpec This methods returns true if both {@link #graph} and {@link #transpose} have copiable
	 *           iterators.
	 */
    @Override
    public boolean hasCopiableIterators() {
		return graph.hasCopiableIterators() && transpose.hasCopiableIterators();
    }

    @Override
    public BidirectionalImmutableGraph copy() {
		return new BidirectionalImmutableGraph(this.graph.copy(), this.transpose.copy());
    }

    /**
	 * Returns a view on the transpose of this bidirectional graph. Successors become predecessors, and
	 * vice-versa.
	 *
	 * @apiNote Note that the returned {@link BidirectionalImmutableGraph} is just a view. Thus, it
	 *          cannot be accessed concurrently with this bidirectional graph.
	 *
	 * @return a view on the transpose of this bidirectional graph.
	 */
    public BidirectionalImmutableGraph transpose() {
		return new BidirectionalImmutableGraph(transpose, graph);
    }

    /**
	 * Returns a view on the symmetrized version of this bidirectional graph.
	 *
	 * @apiNote Note that the returned {@link BidirectionalImmutableGraph} is just a view. Thus, it
	 *          cannot be accessed concurrently with this bidirectional graph.
	 *
	 * @implSpec This methods returns the (lazy)
	 *           {@linkplain Transform#union(ImmutableGraph, ImmutableGraph) union} of the graph and its
	 *           transpose. This is equivalent to forgetting the directionality of the arcs: the
	 *           successors of a node are also its predecessors.
	 *
	 * @return the symmetrized version of this bidirectional graph.
	 */
    public BidirectionalImmutableGraph symmetrize() {
		final ImmutableGraph symmetric = Transform.union(graph, transpose);
        return new BidirectionalImmutableGraph(symmetric, symmetric);
    }

    /**
	 * Returns a view on the simple (loopless and symmetric) version of this bidirectional graph.
	 *
	 * @apiNote Note that the returned {@link BidirectionalImmutableGraph} is just a view. Thus, it
	 *          cannot be accessed concurrently with this bidirectional graph.
	 *
	 * @implSpec This methods returns the (lazy) result of
	 *           {@linkplain Transform#simplify(ImmutableGraph, ImmutableGraph)} on the graph and its
	 *           transpose. Beside forgetting directionality of the arcs, as in {@link #symmetrize()},
	 *           loops are removed.
	 *
	 * @return the simple (symmetric and loopless) version of this bidirectional graph.
	 */
    public BidirectionalImmutableGraph simplify() {
		final ImmutableGraph simplified = Transform.simplify(graph, transpose);
        return new BidirectionalImmutableGraph(simplified, simplified);
    }

	/**
	 * {@inheritDoc}
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#outdegree(long)} on
	 *           {@link #graph}.
	 */
    @Override
    public long outdegree(final long l) {
		return graph.outdegree(l);
    }

	/**
	 * {@inheritDoc}
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#successors(long)} on
	 *           {@link #graph}.
	 */
	@Override
	public LazyLongIterator successors(final long nodeId) {
		return graph.successors(nodeId);
	}

	/**
	 * {@inheritDoc}
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#successorBigArray(long)} on
	 *           {@link #graph}.
	 */

	@Override
	public long[][] successorBigArray(final long x) {
		return graph.successorBigArray(x);
	}

	/**
	 * Returns the indegree of a node
	 *
	 * @param x a node.
	 * @return the indegree of {@code x}.
	 */

	public long indegree(final long x) {
		return transpose.outdegree(x);
    }

	/**
	 * Returns a lazy iterator over the successors of a given node. The iteration terminates when -1 is
	 * returned.
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#successors(long)} on
	 *           {@link #transpose}.
	 *
	 * @param x a node.
	 * @return a lazy iterator over the predecessors of the node.
	 */
	public LazyLongIterator predecessors(final long x) {
		return transpose.successors(x);
    }

	/**
	 * Returns a reference to a big array containing the predecessors of a given node.
	 *
	 * <P>
	 * The returned big array may contain more entries than the outdegree of <code>x</code>. However,
	 * only those with indices from 0 (inclusive) to the indegree of <code>x</code> (exclusive) contain
	 * valid data.
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#successorBigArray(long)} on
	 *           {@link #transpose}.
	 *
	 * @param x a node.
	 * @return a big array whose first elements are the successors of the node; the array must not be
	 *         modified by the caller.
	 */
	public long[][] predecessorBigArray(final long x) {
		return transpose.successorBigArray(x);
    }

	/**
	 * Returns an iterator enumerating the outdegrees of the nodes of this graph.
	 *
	 * @implSpec This implementation just invokes {@link ImmutableGraph#outdegrees()} on
	 *           {@link #transpose}.
	 *
	 * @return an iterator enumerating the outdegrees of the nodes of this graph.
	 */
    public LongIterator indegrees() {
		return transpose.outdegrees();
    }
}
